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
# from multiprocessing.dummy import Pool
from multiprocessing import Pool

import memcache

import appsinstalled_pb2

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


def process_files(files, device_memc, dry):
    with Pool(processes=3) as pool:
        pool.starmap(process_file, [(fn, device_memc, dry) for fn in files])


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    script_start_time = time.time()
    files = glob.iglob(options.pattern)
    process_files(files, device_memc, options.dry)
    logging.info('Script finished in {0:.2f} seconds'.format(
        time.time() - script_start_time))


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log,
                        level=logging.DEBUG if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
