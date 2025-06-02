def internal_id(id: dict) -> str:
  return f"internal_id({id['table']},{id['offset']})"

class Entity:
  def __init__(self, data):
    self._data = data

  def _match_as(self, var: str) -> str:
    return f"({var} {{_ID:{self._id_internal}}})"

  @property
  def _id_internal(self):
    return internal_id(self._data['_id'])

  @property
  def _id_str(self):
    id = self._data['_id']
    return f"{id['table']}:{id['offset']}"

  @property
  def _src(self):
    print('reading src', self._data['_src'])
    return Entity({'_id': self._data['_src']})

  @property
  def _dst(self):
    return Entity({'_id': self._data['_dst']})

  def __getattr__(self, name):
    return self._data.get(name, None)

  def __repr__(self):
    return self._data.__repr__()
