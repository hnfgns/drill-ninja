__author__ = 'hgunes'
import logging
import requests

logger = logging.getLogger(__name__)


class Response(object):
  def __init__(self, data, error=False, msg=None):
    self.data = data
    self.error = error
    self.msg = msg

  def __str__(self):
    if self.error:
      return 'ERROR: {}'.format(self.msg)

    return 'data: {}'.format(self.data)

  def __repr__(self):
    return repr(str(self))

  @classmethod
  def fromHttpResponse(cls, resp):
    if resp.status_code == 200:
      return cls(resp.json())
    else:
      return cls([], error=True, msg=resp.text)


class rest(object):

  def __init__(self, path, method='get', argnames=None):
    self.path = path
    self.method = method
    self.argnames = argnames or tuple()

  def __call__(self, func):

    def _rest_call(bit, *args, **kargs):
      try:
        path = self.path
        if self.argnames:
          nargs = dict(zip(self.argnames, args))
          path = path.format(**nargs)

        resource = '{}{}'.format(bit.host, path)
        opts = func(bit, *args, **kargs) or {}
        if self.method.lower() == 'get':
          return Response.fromHttpResponse(requests.get(resource, **opts))
        elif self.method.lower() == 'post':
          return Response.fromHttpResponse(requests.post(resource, json=opts))
        else:
          raise 'Unsupported method', self.method
      except Exception as e:
        logger.error('error while making rest call', e)

    return _rest_call


class Drillbit(object):
  LOCAL_HOST = 'http://localhost:8047'
  QUERY_PATH = '/query.json'
  PLUGIN_DETAILS = '/storage/{name}.json'
  PLUGIN_UPLOAD = '/storage/{name}.json'

  def __init__(self, host = LOCAL_HOST):
    self.host = host

  def _path(self, portion):
    return '{}{}'.format(self.host, portion)

  @rest(QUERY_PATH, method='post')
  def query(self, stmt, kind='sql'):
    return {
      "queryType": kind,
      "query": stmt
    }

  @rest(PLUGIN_DETAILS, argnames=('name',))
  def getPlugin(self, name):
    return {}

  @rest(PLUGIN_DETAILS, method='post', argnames=('name',))
  def uploadPlugin(self, name, **config):
    return {
      "name" : name,
      "config" : config
    }
