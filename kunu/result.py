from . import kuzu
from .entity import Entity

class Result:
  _query_result : kuzu.QueryResult

  def __init__(self, query_result : kuzu.QueryResult):
    self._query_result = query_result

  def __iter__(self):
    return self

  def __next__(self):
    if not self._query_result.has_next():
      raise StopIteration

    row = self._query_result.get_next()
    return [
      Entity(c) if isinstance(c, dict) else c for c in row
    ]
