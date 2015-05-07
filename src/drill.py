__author__ = 'hgunes'
import requests

class Response(object):
  def __init__(self, data, error=False, msg=None):
    self.data = data
    self.error = error
    self.msg = msg

  def __str__(self):
    if self.error:
      return 'ERROR: {}'.format(self.msg)

    return '{} column(s) and {} row(s)'.format(len(self.data['columns']), len(self.data['rows']))

  def __repr__(self):
    return repr(str(self))

  @classmethod
  def fromHttpResponse(cls, resp):
    if resp.status_code == 200:
      return cls(resp.json())
    else:
      return cls([], error=True, msg=resp.text)

class Drillbit(object):
  LOCAL_HOST = 'http://localhost:8047'
  QUERY_PATH = '/query.json'

  def __init__(self, host = LOCAL_HOST):
    self.host = host

  def _path(self, portion):
    return '{}{}'.format(self.host, portion)

  def query(self, stmt, kind='sql'):
    opts = {
      "queryType": kind,
      "query": stmt
    }

    return Response.fromHttpResponse(requests.post(self._path(self.QUERY_PATH), json=opts))

