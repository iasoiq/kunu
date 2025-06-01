import sys
import os

TESTDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(TESTDIR, '..'))

import kunu

k = kunu.Kunu(":memory:")

k.execute('create node table Being (name string, primary key (name))')

print('> show tables')
for r in k.execute("call show_tables() return *"):
  print(r)

k.execute('create (:Being { name: "Justin" })')
k.execute('create (:Being { name: "Ellie" })')
k.execute('create (:Being { name: "Katie" })')
k.execute('create (:Being { name: "Milo" })')

print('> all users')
for r in k.execute("match (x:Being) return *"):
  print(r)
  for c in r:
    print('_id:', c._id)
    print('id:', c.id)
    print('internal_id:', c.internal_id)
    print('_label:', c._label)
    print('name:', c.name)
    print('age:', c.age)
    print('<non-existent-property>:', c.missing)
