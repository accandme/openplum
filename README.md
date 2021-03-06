OpenPlum is a query engine able to execute arbitrary SQL queries on an arbitrary number of independent PostgreSQL worker nodes on which a database is partitioned. The engine acts as a layer on top of the worker nodes, interacting with the user via a command-line interface and abstracting away the underlying partitioning. Besides extensive query syntax and nested queries, the engine also handles aggregate functions, which are not currently supported by any other open-source distributed database systems.

## Documentation

OpenPlum documentation can be found in the [wiki].

[wiki]: https://github.com/accandme/openplum/wiki
