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


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key,
                                             str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line = line.decode('UTF-8')
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def process_line(line, device_memc, dry):
    if not line:
        return
    line = line.strip()
    appsinstalled = parse_appsinstalled(line)
    if not appsinstalled:
        raise Exception('No appsinstalled')
    memc_addr = device_memc.get(appsinstalled.dev_type)
    if not memc_addr:
        raise Exception("Unknow device type: %s" % appsinstalled.dev_type)
    ok = insert_appsinstalled(memc_addr, appsinstalled, dry)
    if not ok:
        raise Exception('Failed to insert appsinstalled')


def process_file(fn, device_memc, dry):
    processed = errors = 0
    logging.info('Processing %s' % fn)
    fd = gzip.open(fn)
    lines_counter = 0
    start_time = time.time()
    for line in fd:
        if lines_counter > 15000:
            break
        try:
            process_line(line, device_memc, dry)
        except Exception as e:
            logging.exception(e)
            errors += 1
        else:
            processed += 1
        lines_counter += 1
        if lines_counter % 5000 == 0:
            logging.info('{} lines passed with {} errors'.format(
                lines_counter, errors))
            logging.info(
                'Time for processing 5000 lines: {0:.2f} seconds'.format(
                    time.time() - start_time))
            start_time = time.time()
    if not processed:
        fd.close()
        dot_rename(fn)
        logging.info('file {} was not processed'.format(fn))

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (
            err_rate, NORMAL_ERR_RATE))
    fd.close()
    # TODO: uncomment after tests
    # dot_rename(fn)


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
        while not self._stop.is_set():
            if self.src_queue.empty():
                time.sleep(0.2)
                continue
            line = self.src_queue.get()
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
        memc_clients = {memc_addr: memcache.Client([memc_addr])
                        for dev_type, memc_addr in self.device_memc.items()}
        while not self.__stop:
            if self.queue.empty():
                time.sleep(0.2)
                continue
            memc_addr, key, packed = self.queue.get(timeout=5)
            try:
                if self.dry_run:
                    logging.debug("%s - %s -> %s" % (memc_addr, key,
                                                     packed.replace("\n", " ")))
                else:
                    memc = memc_clients[memc_addr]
                    memc.set(key, packed)
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
        raw_queue.close()
        raw_queue.join_thread()
        parsed_queue.close()
        parsed_queue.join_thread()
    except KeyboardInterrupt:
        # TODO: add proper handling
        sys.exit(1)

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
                        level=logging.DEBUG if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
