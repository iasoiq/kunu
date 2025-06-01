# kunu

A simple wrapper of the [Kuzu](https://kuzudb.com) graph db package.

## Iterable query results

```
k = Kunu("./example-db")
for n in k.execute("match (x) return *"):
  print(n)
```

## Todo
