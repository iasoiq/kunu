import kuzu
from typing import Any

from .utils import single_entity
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

  def find(
    self,
    type: str,
    key: dict[str,Any]
  ) -> Entity:
    if len(key) == 0:
      raise Exception("Missing key")
    where = ' and '.join([f"x.{k} = ${k}" for k in key])
    res = self._conn.execute(
      f"match (x:{type}) where {where} return x",
      key
    )
    return single_entity(res)

  def get(self, id: str) -> Entity:
    (table, offset) = (int(x) for x in id.split(':'))
    res = self._conn.execute(
      f"match (x {{_ID:internal_id({table},{offset})}}) return x"
    )
    return single_entity(res)

  def get_edge(self, id: str) -> Entity:
    (table, offset) = (int(x) for x in id.split(':'))
    res = self._conn.execute(
      f"match ()-[x {{_ID:internal_id({table},{offset})}}]->() return x"
    )
    return single_entity(res)

  def update(
    self,
    entity: Entity,
    query: str,
    parameters: dict[str, Any] | None = None
  ):
    res = self._conn.execute(
      f"match {entity._match_as('a')} {query} return a",
      parameters
    )
    return single_entity(res)

  def link(
    self,
    src: Entity,
    dst: Entity,
    type: str,
    props: dict = {}
  ) -> Entity:
    set = ', '.join([f"r.{k} = ${k}" for k in props])
    if len(props) > 0:
      set = 'set ' + set

    # @todo: option for create instead of merge?
    res = self._conn.execute(
      f"""
        match {src._match_as('a')}, {dst._match_as('b')}
        merge (a)-[r:{type}]->(b)
        {set}
        return r
      """,
      props
    )

    # the returned edge does not have the permanent internal id offset
    # (see https://github.com/kuzudb/kuzu/issues/5481)
    # we could do something like this, but it would be subject to a race
    # condition if another thread created a similar edge in between
    # these two transactions.
    # match ({src})-[r:{type}]->({dst}) return r order by offset(id(r)) desc limit 1;

    return single_entity(res)
