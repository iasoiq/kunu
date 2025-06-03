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

print('> all ids')
for r in k.execute("match (a) return id(a)"):
  print(r[0], r[0].table, r[0].offset)

print('> find nodes by key')
j = k.find('Being', {'name': 'Justin'})
print(j)
print('<non-existent-property>:', j.missing)

h = k.find('Type', {'name': 'Human'})
print(h)

print('> get node by id')
print(k.get("0:3"))
print(k.get("1:1"))

print('> update node')
r = k.update(j, "set a.mass = $mass", {'mass': 100})
print(r)

print('> link nodes')
r = k.link(j, h, 'IsA')
print(r)
# id offset is wrong (always returns 2**64) https://github.com/kuzudb/kuzu/issues/5481
print('_id_str:', r._id_str)
print('_src:', r._src)
print('_src._id_str:', r._src._id_str)
print('_dst:', r._dst)
print('_dst._id_internal:', r._dst._id_internal)

r = k.link(
  k.find('Being', {'name': 'Ellie'}),
  k.find('Type', {'name': 'Dog'}),
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
