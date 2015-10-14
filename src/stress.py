#!/usr/bin/env python
__author__ = 'hgunes'
import argparse
import logging
import sys
import concurrent
import drill

logger = logging.getLogger(__name__)

def run(opts):
  logger.info('running with options %s', opts)
  bit = drill.Drillbit()
  pool = concurrent.ThreadPool(opts.concurrency)

  def query(i, stmt):
    logger.info('REQ %s', i)
    resp = bit.query(stmt)
    logger.info('RESP #%d: %s', i, resp)

  stmt = 'select * from cp.`employee.json` limit 1'
  pool.start()

  cur, total = 0, opts.requests
  while True:
    if not (total == 0 or cur < total): break
    cur += 1
    pool.execute(query, cur, stmt)

  pool.stop()
  pool.join()

def parseOptions(args):
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', action='store', dest='concurrency', default=10, type=int, required=0)
  parser.add_argument('-r', action='store', dest='requests', default=100, type=int, required=0, help='total # of requests. 0 for infinity')
  return parser.parse_args(args)

if __name__ == '__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.INFO)
  opts = parseOptions(sys.argv[1:])
  run(opts)
