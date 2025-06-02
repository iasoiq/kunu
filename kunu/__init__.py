import kuzu
from typing import Any

from .result import Result
from .entity import Entity, internal_id

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

  # @todo: turn `find` into 0+ results, and add `get` for the single result case
  def find(
    self,
    query: str | kuzu.PreparedStatement,
    parameters: dict[str, Any] | None = None
  ) -> Entity|None:
    r = self._conn.execute(query, parameters)
    if not r.has_next():
      return None
    return Entity(r.get_next()[0])

  def update(
    self,
    entity: Entity,
    query: str,
    parameters: dict[str, Any] | None = None
  ):
    r = self._conn.execute(
      f"match {entity._match_as('a')} {query} return a",
      parameters
    )
    if not r.has_next():
      raise Exception(f"No result for update on {entity._id_str}")
    return Entity(r.get_next()[0])

  # @todo: option for non-merge?
  def link(
    self,
    src: Entity,
    dst: Entity,
    type: str,
    props: dict = {}
  ) -> Entity:
    r = self._conn.execute(
      f"""
        match {src._match_as('a')}, {dst._match_as('b')}
        merge (a)-[r:{type}]->(b)
        return r
      """
      # set r = $p
      # { 'p': props }
    )
    if not r.has_next():
      raise Exception(f"No result for link on {src._id_str}-[:{type}]->{dst._id_str}")
    e = r.get_next()
    if r.has_next():
      raise Exception('Multiple edges returned')
    return Entity(e[0])
