import kuzu
from typing import Any

from .utils import single_entity, single_entity_or_none
from .result import Result
from .entity import Entity, internal_id

class Kunu:
  _db : kuzu.Database
  _conn : kuzu.Connection
  _primary_keys : dict[str,str] = {}

  def __init__(self, file: str):
    self._db = kuzu.Database(file)
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

  def _pk_for(self, type: str) -> str:
    pk = self._primary_keys.get(type)
    if pk is not None:
      return pk
      pk = self._primary_keys()
    r = self._conn.execute(f"""
      call table_info('{type}') where `primary key` = true return name
    """)
    pk = r.get_next()[0]
    if pk is None:
      raise Exception(f'No primary key found for {type}')
    self._primary_keys[type] = pk
    return pk

  def get(self, id: str) -> Entity:
    (table, offset) = (int(x) for x in id.split(':'))
    res = self._conn.execute(
      f"match (x {{_ID:internal_id({table},{offset})}}) return x"
    )
    return single_entity(res)

  def find_by_pk(
    self,
    type: str,
    value: Any
  ) -> Entity|None:
    pk = self._pk_for(type)
    res = self._conn.execute(
      f"match (x:{type} {{{pk}:$value}}) return x",
      {'value': value}
    )
    return single_entity_or_none(res)

  def get_by_pk(
    self,
    type: str,
    value: Any
  ) -> Entity:
    res = self.find_by_pk(type, value)
    if res is None:
      raise Exception('Missing entity')
    return res

  def create(
    self,
    type: str,
    value: Any,
    properties: dict[str,Any]|None = None
  ) -> Entity:
    pk = self._pk_for(type)
    res = self._conn.execute(
      f"create (x:{type} {{{pk}:$value}}) return x",
      {'value': value}
    )
    entity = single_entity(res)
    if properties is None:
      return entity
    return self.update(entity, properties)

  def get_edge(self, id: str) -> Entity:
    (table, offset) = (int(x) for x in id.split(':'))
    res = self._conn.execute(
      f"match ()-[x {{_ID:internal_id({table},{offset})}}]->() return x"
    )
    return single_entity(res)

  def modify(
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

  def update(
    self,
    entity: Entity,
    properties: dict[str,Any]
  ):
    modification = ', '.join(
      f"a.{prop}=${prop}" for prop in properties
    )
    return self.modify(entity, f"SET {modification}", properties)

  def remove(
    self,
    entity: Entity
  ):
    return self.modify(entity, "detach delete a")

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

  def orphans(self, type: str) -> Result:
    return self.execute(f"""
      match (n:{type}) where not (n)--() return n
    """)
