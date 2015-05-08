__author__ = 'hgunes'
import logging
import sys
import concurrent
import drill

logger = logging.getLogger(__name__)

def run(args):
  bit = drill.Drillbit()
  pool = concurrent.ThreadPool(10)

  def query(i, stmt):
    logger.info('REQ %s', i)
    resp = bit.query(stmt)
    logger.info('RESP #%d: %s', i, resp)

  stmt = 'select * from cp.`employee.json` limit 1'
  pool.start()

  for i in xrange(100):
    pool.execute(query, i, stmt)

  pool.stop()
  pool.join()


if __name__ == '__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.INFO)
  run(sys.argv)