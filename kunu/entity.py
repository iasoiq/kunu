class Entity:
  def __init__(self, data):
    self._data = data

  # @todo: wrap _src and _dst in Entity, if present

  @property
  def id(self):
    id = self._id
    return f"{id['table']}:{id['offset']}"

  @property
  def internal_id(self):
    id = self._id
    return f"internal_id({id['table']},{id['offset']})"

  def __getattr__(self, name):
    return self._data.get(name, None)

  def __repr__(self):
    return self._data.__repr__()
