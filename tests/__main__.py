import sys
import os

TESTDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(TESTDIR, '..'))

import kunu

k = kunu.Kunu(":memory:")

k.execute('create node table Being (name string, mass uint16, primary key (name))')
k.execute('create node table Type (name string, primary key (name))')
k.execute('create rel table IsA (from Being to Type, exemplar boolean)')

print('> show tables')
for r in k.execute("call show_tables() return *"):
  print(r)

k.execute("""
  create
    (:Being { name: "Justin" }),
    (:Being { name: "Ellie" }),
    (:Being { name: "Katie" }),
    (:Being { name: "Milo" }),
    (:Type { name: "Human" }),
    (:Type { name: "Dog" })
""")

print('> all beings')
for r in k.execute("match (x:Being) return *"):
  print(r)
  for c in r:
    print('_id:', c._id)
    print('_id_internal:', c._internal_id)
    print('_label:', c._label)
    print('name:', c.name)
    print('<non-existent-property>:', c.missing)

print('> all ids')
for r in k.execute("match (a) return id(a)"):
  print(r[0], r[0].table, r[0].offset)

print('> get node by id')
print(k.get("0:3"))
print(k.get("1:1"))

print('> get node by pk')
print(k.get_by_pk('Being', 'Ellie'))
print(k.get_by_pk('Being', 'Katie'))

print('> find node by pk')
print(k.find_by_pk('Being', 'Milo'))
print('none', k.find_by_pk('Being', 'Mike'))

print('> modify node')
j = k.get_by_pk('Being', 'Justin')
r = k.modify(j, "set a.mass = $mass", {'mass': 100})
print(r)

print('> update node')
m = k.get_by_pk('Being', 'Milo')
r = k.update(m, {'mass': 10})
print(r)

print('> link nodes')
h = k.get_by_pk('Type', 'Human')
r = k.link(j, h, 'IsA')
print(r)
# id offset is wrong (always returns 2**62) https://github.com/kuzudb/kuzu/issues/5481
print('_id_str:', r._id_str)
print('_src:', r._src)
print('_src._id_str:', r._src._id_str)
print('_dst:', r._dst)
print('_dst._id_internal:', r._dst._id_internal)

r = k.link(
  k.get_by_pk('Being', 'Ellie'),
  k.get_by_pk('Type', 'Dog'),
  'IsA',
  {'exemplar': True}
)
print(r)
print('src:', k.get(r._src._id_str))
print('dst:', k.get(r._dst._id_str))

print('> edge by id')
print(k.get_edge("2:1"))

print('> all nodes')
for r in k.execute("match (x) return x"):
  print(r)

print('> all edges')
for r in k.execute("match ()-[x]->() return x"):
  print(r)

print('> remove node')
r = k.remove(j)
print(r)

print('> create node')
r = k.create('Type', 'Cat')
print(r)

print('> all nodes')
for r in k.execute("match (x) return x"):
  print(r)

print('> all edges')
for r in k.execute("match ()-[x]->() return x"):
  print(r)
