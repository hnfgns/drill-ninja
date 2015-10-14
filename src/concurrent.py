__author__ = 'hgunes'
import logging
import Queue as queue
import threading
import time

__all__ = ['ThreadPool']

logger = logging.getLogger(__name__)


class Worker(threading.Thread):

  def __init__(self, pool, *args, **kargs):
    threading.Thread.__init__(self, *args, **kargs)
    self.setDaemon(True)
    self.pool = pool
    self.running = False

  def start(self):
    self.running = True
    threading.Thread.start(self)

  def run(self):
    while True:
      try:
        task, args, kargs = self.pool.tasks.get(block=False, timeout=.01)
        task(*args, **kargs)
      except queue.Empty:
        if not self.running:
          break
      finally:
        time.sleep(.01)

  def stop(self):
    self.running = False


class ThreadPool(object):

  def __init__(self, numWorkers):
    self.tasks = queue.Queue()
    self.workers = [Worker(self) for _ in xrange(numWorkers)]
    self.running = False

  def execute(self, task, *args, **kargs):
    if self.running:
      self.tasks.put((task, args, kargs,))

  def start(self):
    self.running = True
    for worker in self.workers:
      worker.start()

  def stop(self):
    for worker in self.workers:
      worker.stop()
    self.running = False

  def join(self):
    while self.workers:
      for worker in self.workers:
        worker.join(.05)
        if not worker.isAlive():
          self.workers.remove(worker)
      time.sleep(.1)

