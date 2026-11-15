# VoltDB

A SQLancer provider for [VoltDB](https://www.voltactivedata.com/), an in-memory, partitioned, ACID SQL database.
Each database it generates has replicated and partitioned tables, indexes, materialized views, stored procedures
and streams. It fills them with single-row inserts, upserts, updates and deletes, then runs the chosen oracles
against them through the VoltDB JDBC driver (`org.voltdb:voltdbclient`).

## Running it

Start a VoltDB server, then:

```
java -jar target/sqlancer-*.jar --num-threads 4 --username "" --password "" voltdb --oracle NOREC,TLP_WHERE,PQS
```

The client port is 21212 by default (`--host`, `--port`). For a cluster, `--servers host:port,host:port,...`
connects to every node, so the run continues while any one node is down. With security on, give an administrator's
`--username` and `--password`.

## Oracles

| Oracle | What it checks |
| --- | --- |
| `NOREC` (default) | Non-optimizing Reference Engine Construction: a filtered query against the count of rows its predicate is true for. |
| `TLP_WHERE`, `TLP_DISTINCT`, `TLP_HAVING`, `TLP_AGGREGATE` | Ternary Logic Partitioning of WHERE and HAVING clauses, with DISTINCT, and of aggregates. |
| `PQS` | Pivoted Query Synthesis: a query built to return a chosen row must return it. The provider evaluates VoltDB's expressions itself to build the query. |
| `PARAMS` | A query with literals against the same query with JDBC parameters (`?`). |
| `VIEWS` | Each materialized view against the query it materializes, after the database's changes. |
| `LIMIT` | `LIMIT` and `OFFSET` against slices of the full, ordered result. |
| `SUBQUERY` | A query with an `IN`, `ANY` or `ALL` subquery against the same query with the subquery written with `EXISTS`. |
| `WINDOW` | A window function's values against values computed in Java from the rows of its own query. |
| `SETOPS` | `UNION`, `INTERSECT` and `EXCEPT`, with and without `ALL`, against the same operation done in Java on the operands' rows. |
| `DERIVED` | A query against itself wrapped in a derived table, with its predicate pushed into one, or written as a common table expression. |
| `PROCEDURE` | A query run ad hoc against the same query as a stored procedure (`CREATE PROCEDURE ... AS`), called with its literals as parameters. |
| `SCHEMA` | Queries return the same rows when planned, from the plan cache, after a change to their table, and after the change is undone. |
| `GEO` | Geospatial functions over `GEOGRAPHY_POINT` and `GEOGRAPHY` columns against the same geography computed in Java. |
| `PLANS` | Differential query plans: the same query's rows when it is planned two ways (an index made unusable, another join order, an added index). |
| `STREAMS` | Rows inserted into streams against their materialized views' totals and an ordinary shadow table. |
| `PERMS` | With `--security true`: what a read-only and a read-write role may and may not do. |
| `DURABLE` | With `--crashes true`: the rows that existed before the whole cluster was killed and recovered. |
| `XDCR` | With `--xdcr`: two clusters linked by cross-datacenter replication hold the same rows once replication settles. |
| `DIFF` | With `--mirror`: the same queries against a second cluster, normally another VoltDB version. |
| `SMOKE` | Every table can be read; for checking a setup. |

Several oracles can be given, comma-separated; the provider cycles through them, and `--use-reducer` reduces a
failure from any of them.

## Options

| Option | Meaning |
| --- | --- |
| `--max-num-tables`, `--max-num-indexes`, `--max-num-updates`, `--max-num-deletes`, `--max-num-views` | Upper bounds on what each database creates and issues. |
| `--avoid-known-bugs true\|false` | Do not generate SQL that hits a known, unfixed VoltDB bug (default true). |
| `--retry-seconds N` | How long a statement that VoltDB rejects because of a concurrent schema change is retried. |
| `--servers host:port,...` | Every node's client address, instead of `--host` and `--port`. |
| `--node-failures`, `--resizes`, `--crashes`, `--upgrades` `true\|false` | The cluster is changed under the run: nodes killed and rejoined, grown and shrunk elastically, all killed and recovered, or upgraded in service. The provider expects, retries or skips the errors these cause, and skips a statement whose outcome a failure left unknown. |
| `--xdcr host:port,...` | A second cluster linked by XDCR, for the `XDCR` oracle. |
| `--mirror host:port,...` | A second cluster for the `DIFF` oracle. Every change is repeated on it. |
| `--security true\|false` | Security is on, for the `PERMS` oracle. |
| `--export-target NAME` | The export target that the `STREAMS` oracle's streams export to. |
| `--ddl-hold FILE` | While this file exists, DDL statements wait. |

Making those cluster changes (killing nodes, resizing, upgrading, linking two clusters) is the job of a harness
outside SQLancer. Volt Active Data's harness, with its nightly configuration and a minimal reproduction of every bug
the provider has found, is in [VoltDB/sqlancer](https://github.com/VoltDB/sqlancer) under `voltdb/`.

## Tests

`test/sqlancer/dbms/TestVoltDB.java` runs the main oracles against a server on localhost:21212 with security off,
when the environment has `VOLTDB_AVAILABLE=true`.
