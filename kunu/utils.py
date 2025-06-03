from kuzu import QueryResult
from .entity import Entity

def single_entity(result: QueryResult) -> Entity:
  if not result.has_next():
    raise Exception('Missing entity')
  row = result.get_next()
  if result.has_next():
    raise Exception('Multiple entities returned')
  return Entity(row[0])
