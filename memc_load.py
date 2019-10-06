#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import glob
import gzip
import logging
import os
import sys
import time
from optparse import OptionParser
import queue
from multiprocessing.dummy import Pool
from multiprocessing import Queue, Process, Event
# from multiprocessing import Pool
from threading import Thread
# from signal import signal, SIGPIPE, SIG_DFL

import memcache

import appsinstalled_pb2

# signal(SIGPIPE, SIG_DFL)

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


class FilesReader(object):

    def __init__(self, queue):
        self.queue = queue

    def read_file(self, filename):
        start_time = time.time()
        logging.info('Processing %s' % filename)
        with gzip.open(filename) as fd:
            lines_counter = 0
            for line in fd:
                if lines_counter > 15000:
                    break
                self.queue.put(line, timeout=15)
                lines_counter += 1
                if lines_counter % 5000 == 0:
                    logging.info(
                        'Read 5000 lines in: {0:.2f} seconds'.format(
                            time.time() - start_time))
                    start_time = time.time()
        # dot_rename(filename)

    def __call__(self, files):
        with Pool(processes=3) as pool:
            try:
                pool.map(self.read_file, [fn for fn in files])
            except KeyboardInterrupt:
                logging.info('Shut down pool')
        logging.info('FilesReader finished')


class LineParser(Process):

    def __init__(self, src_queue, parsed_queue, device_memc,
                 *args, **kwargs):
        self.src_queue = src_queue
        self.parsed_queue = parsed_queue
        self.device_memc = device_memc
        self._stop = Event()
        super().__init__(*args, **kwargs)

    def _parse_appsinstalled(self, line):
        # TODO: refactor as factory method of Appsinstalled class
        if not line:
            raise Exception('Empty line')
        line = line.strip()
        line = line.decode('UTF-8')
        line_parts = line.strip().split("\t")
        if len(line_parts) < 5:
            raise Exception('Too few line parts. 5 or more expected')
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
            raise Exception('No dev_type or dev_id')
        try:
            apps = [int(a.strip()) for a in raw_apps.split(",")]
        except ValueError:
            apps = [int(a.strip())
                    for a in raw_apps.split(",") if a.isidigit()]
            logging.info("Not all user apps are digits: `%s`" % line)
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info("Invalid geo coords: `%s`" % line)
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)

    def _serialize(self, appsinstalled):
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        return key, packed

    def _get_memc_addr(self, appsinstalled):
        memc_addr = self.device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            raise Exception('No memc_addr')
        return memc_addr

    def parse_lines(self):
        counter = successes = fails = 0
        start_time = time.time()
        while not self._stop.is_set() or not self.src_queue.empty():
            try:
                line = self.src_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.2)
                continue
            try:
                appsinstalled = self._parse_appsinstalled(line)
                memc_addr = self._get_memc_addr(appsinstalled)
                key, packed = self._serialize(appsinstalled)
            except Exception as e:
                logging.exception(e)
                fails += 1
            else:
                successes += 1
                self.parsed_queue.put((memc_addr, key, packed), timeout=15)
            counter += 1
            if counter % 5000 == 0:
                avg_time = (time.time() - start_time) / 5000
                logging.info('Parsed 5000 lines. Avg time: {}'.format(
                    avg_time))
        logging.info('Parser finished')
        err_rate = float(fails) / successes
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable processing error rate (%s)" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed process" % (
                err_rate, NORMAL_ERR_RATE))

    def run(self):
        self.parse_lines()

    def stop(self):
        self._stop.set()


class BufferedMemcLoader(object):

    def __init__(self, device_memc, buff_size=1000):
        self.clients = {memc_addr: memcache.Client([memc_addr])
                        for _, memc_addr in device_memc.items()}
        self.buffs = {memc_addr: [] for _, memc_addr in device_memc.items()}
        self.buff_size = buff_size

    def drain_buff(self, memc_addr):
        memc_client = self.clients[memc_addr]
        buff = self.buffs[memc_addr]
        for key, value in buff:
            memc_client.set(key, value)
        del buff[:]

    def load(self, memc_addr, key, value):
        buff = self.buffs[memc_addr]
        buff.append((key, value))
        if len(buff) == self.buff_size:
            logging.debug('{} buffer is full, draining'.format(memc_addr))
            self.drain_buff(memc_addr)

    def flush(self):
        for memc_addr in self.clients:
            self.drain_buff(memc_addr)


class MemcachedPoster(Thread):

    def __init__(self, queue, device_memc, dry_run, *args, **kwargs):
        self.__stop = False
        self.queue = queue
        self.device_memc = device_memc
        self.dry_run = dry_run
        super().__init__(*args, **kwargs)

    def run(self):
        start_time = time.time()
        counter = successes = fails = 0
        memc_loader = BufferedMemcLoader(self.device_memc)
        while not self.__stop or not self.queue.empty():
            try:
                memc_addr, key, packed = self.queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.2)
                continue
            try:
                if self.dry_run:
                    logging.debug("%s - %s -> %s" % (
                        memc_addr, key,
                        packed.replace("\n", " "))
                    )
                else:
                    memc_loader.load(memc_addr, key, packed)
            except Exception as e:
                logging.exception(e)
                fails += 1
            else:
                successes += 1
            counter += 1
            if counter % 5000 == 0:
                avg_time = (time.time() - start_time) / 5000
                logging.info('Inserted 5000 lines. Avg time: {}'.format(
                    avg_time))
                logging.debug('Put data example {}:{}'.format(key, packed))
        memc_loader.flush()
        logging.info('Finished insertion')
        err_rate = float(fails) / successes
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable processing error rate (%s)" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed process" % (
                err_rate, NORMAL_ERR_RATE))

    def stop(self):
        self.__stop = True


class WorkCrew(object):

    def __init__(self, num_workers, worker_cls, *wrk_args, **wrk_kwargs):
        self.num_workers = num_workers
        self.worker_cls = worker_cls
        self.crew = [worker_cls(*wrk_args, **wrk_kwargs)
                     for _ in range(num_workers)]

    def start(self):
        for worker in self.crew:
            worker.start()

    def stop(self):
        for worker in self.crew:
            worker.stop()

    def join(self):
        for worker in self.crew:
            worker.join()


def main(options):
    script_start_time = time.time()
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    raw_queue = Queue(maxsize=2000)
    parsed_queue = Queue(maxsize=2000)

    files = glob.iglob(options.pattern)
    freader_thr = Thread(target=FilesReader(raw_queue), args=(files,))
    freader_thr.start()

    parser_crew = WorkCrew(2, LineParser, raw_queue, parsed_queue, device_memc)
    parser_crew.start()

    poster_crew = WorkCrew(3, MemcachedPoster, parsed_queue, device_memc,
                           options.dry)
    poster_crew.start()

    try:
        freader_thr.join()
        parser_crew.stop()
        parser_crew.join()
        poster_crew.stop()
        poster_crew.join()
        logging.debug('Workers stopped')
        if not raw_queue.empty():
            logging.info('raw queue is not empty')
        if not parsed_queue.empty():
            logging.info('parsed queue is not empty')
    except KeyboardInterrupt:
        sys.exit()
    logging.info('Script finished in {0:.2f} seconds'.format(
        time.time() - script_start_time))


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store",
                  default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log,
                        level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
