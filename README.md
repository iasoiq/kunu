# kunu

A simple wrapper of the [Kuzu](https://kuzudb.com) graph db package.

## Iterable query results
```
k = Kunu("./example-db")
for n in k.execute("match (x) return *"):
  print(n)
```

## Get node by id
```
k.get("1:1")
=> {'_id': {'offset': 1, 'table': 1}, '_label': 'Type', 'name': 'Dog', 'mass': None}
```

## Get node by type and primary key
```
k.get_by_pk('Being', 'Justin')
=> {'_id': {'offset': 0, 'table': 0}, '_label': 'Being', 'name': 'Justin', 'mass': None}
```

## Find (if present) node by type and primary key
```
k.find_by_pk('Being', 'Justin')
=> {'_id': {'offset': 0, 'table': 0}, '_label': 'Being', 'name': 'Justin', 'mass': None}
```
```
k.find_by_pk('Being', 'Nonexistent')
=> None
```

## Modify node
```
node = k.find('Being', {'name': 'Justin'})
k.update(node, "set a.mass = $mass", {'mass': 100})
```

## Update node
```
node = k.find('Being', {'name': 'Justin'})
k.update(node, {'mass': 20})
```

## Create node
```
k.create('Type', 'Cat')
=> {'_id': {'offset': 2, 'table': 1}, '_label': 'Type', 'name': 'Cat'}
```

## Remove (and detach) node
```
node = k.find('Being', {'name': 'Justin'})
k.remove(node)
```

## Link nodes
```
k.link(
  k.find('Being', {'name': 'Ellie'}),
  k.find('Type', {'name': 'Dog'}),
  'IsA',
  {'exemplar': True}
)
```

## Get edge by id
```
k.get_edge("2:1")
=> {'_src': {'offset': 1, 'table': 0}, '_dst': {'offset': 1, 'table': 1}, '_label': 'IsA', '_id': {'offset': 1, 'table': 2}, 'exemplar': True}
```

## Using `_src` and `_dst` on edge

```
k.get(edge._src._id_str)
=> {'_id': {'offset': 1, 'table': 0}, '_label': 'Being', 'name': 'Ellie', 'mass': None}

k.get(edge._dst._id_str)
=> {'_id': {'offset': 1, 'table': 1}, '_label': 'Type', 'name': 'Dog', 'mass': None}
```
