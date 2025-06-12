from kuzu import QueryResult
from .entity import Entity

def single_entity(result: QueryResult) -> Entity:
  if not result.has_next():
    raise Exception('Missing entity')
  row = result.get_next()
  if result.has_next():
    raise Exception('Multiple entities returned')
  return Entity(row[0])

def single_entity_or_none(result: QueryResult) -> Entity|None:
  if not result.has_next():
    return None
  row = result.get_next()
  if result.has_next():
    raise Exception('Multiple entities returned')
  return Entity(row[0])
