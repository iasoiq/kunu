import kuzu
from typing import Any

from .result import Result

class Kunu:
  _db : kuzu.Database
  _conn : kuzu.Connection

  def __init__(self, dir: str):
    self._db = kuzu.Database(dir)
    self._conn = kuzu.Connection(self._db)

  def close(self):
    self._conn.close()
    self._db.close()

  def execute(
    self,
    query: str | kuzu.PreparedStatement,
    parameters: dict[str, Any] | None = None
  ):
    return Result(self._conn.execute(query, parameters))
