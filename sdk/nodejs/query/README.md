# query.js

query.js makes it easy to write _SQL-like queries_ over _JS data structures_. It supports `Array`,
`Set`, `Map`, and `Object`, as well as `Iterable` more generally. Unlike other query libraries,
query.js has strong support for `Promise`, but is considerably simpler to understand and write than
streaming query models such as RxJS.

query.js values _simplicity_ and _intuition_ over flexibility.

It provides a [LINQ][linq]-like query model---relational, lazily-evaluated, and batch-oriented (vs
streaming).

[linq]: https://en.wikipedia.org/wiki/Language_Integrated_Query
