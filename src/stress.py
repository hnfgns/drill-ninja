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
  bit = drill.Drillbit(opts.host)
  pool = concurrent.ThreadPool(opts.concurrency)
  stats = [0, 0]
  def query(i, stmt):
    # logger.warn('REQ %s', i)
    try:
      resp = bit.query(stmt)
      if resp.error:
        logger.warn('query #%d failed with %s', i, resp.msg)
        raise Exception(resp.msg)
      stats[0] += 1
      logger.warn('query #%d passed with %s', i, resp.data)
    except Exception:
      logger.exception('shit')
      stats[1] += 1

    if i % 100 == 0:
      logger.warn('current: %d -- positive: %d -- negative: %d', i, stats[0], stats[1])

  stmt = 'select * from cp.`employee.json` limit 1'
  stmt = '''
  -- tpch6 using 1395599672 as a seed to the RNG

select
  sum(l_extendedprice * l_discount) as revenue
from
  dfs.`perf`.`lineitem_par100`
where
  l_shipdate >= date '1997-01-01'
  and l_shipdate < date '1997-01-01' + interval '1' year
  and
  l_discount between 0.03 - 0.01 and 0.03 + 0.01
  and l_quantity < 24
  '''
  pool.start()

  cur, total = 0, opts.requests
  while True:
    if not (total == 0 or cur < total): break
    cur += 1
    pool.execute(query, cur, stmt)

  pool.stop()
  pool.join()
  logger.info('current: %d -- positive: %d -- negative: %d', cur, stats[0], stats[1])


def parseOptions(args):
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', action='store', dest='concurrency', default=10, type=int, required=0)
  parser.add_argument('-r', action='store', dest='requests', default=100, type=int, required=0, help='total # of requests. 0 for making this endless')
  parser.add_argument('-host', action='store', dest='host', default='http://localhost:8047', type=str, required=0, help='drillbit endpoint')
  return parser.parse_args(args)

if __name__ == '__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.WARN)
  opts = parseOptions(sys.argv[1:])
  run(opts)
