[![Build Status](https://github.com/sqlancer/sqlancer/workflows/ci/badge.svg)](https://github.com/sqlancer/sqlancer/actions)
[![Twitter](https://img.shields.io/twitter/follow/sqlancer_dbms?style=social)](https://twitter.com/sqlancer_dbms)
# SQLancer


![SQLancer](media/logo/png/sqlancer_logo_logo_pos_500.png)

SQLancer (Synthesized Query Lancer) is a tool to automatically test Database Management Systems (DBMS) in order to find logic bugs in their implementation. We refer to logic bugs as those bugs that cause the DBMS to fetch an incorrect result set (e.g., by omitting a record).

SQLancer operates in the following two phases:

1. Database generation: The goal of this phase is to create a populated database, and stress the DBMS to increase the probability of causing an inconsistent database state that could be detected subsequently. First, random tables are created. Then, randomly SQL statements are chosen to generate, modify, and delete data. Also other statements, such as those to create indexes as well as views and to set DBMS-specific options are sent to the DBMS. **News: we support Query Plan Guidance (QPG) now. See Generation Approaches below.**
2. Testing: The goal of this phase is to detect the logic bugs based on the generated database. See Testing Approaches below. **News: we support Cardinality Estimation Restriction Testing (CERT) oracle now. See Testing Approaches below.**

# Getting Started

Requirements:
* Java 11 or above
* [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)
* The DBMS that you want to test (embedded DBMSs such as DuckDB, H2, and SQLite do not require a setup)

The following commands clone SQLancer, create a JAR, and start SQLancer to test SQLite using Non-optimizing Reference Engine Construction (NoREC):

```
git clone https://github.com/sqlancer/sqlancer
cd sqlancer
mvn package -DskipTests
cd target
java -jar sqlancer-*.jar --num-threads 4 sqlite3 --oracle NoREC
```

If the execution prints progress information every five seconds, then the tool works as expected. Note that SQLancer might find bugs in SQLite. Before reporting these, be sure to check that they can still be reproduced when using the latest development version. The shortcut CTRL+C can be used to terminate SQLancer manually. If SQLancer does not find any bugs, it executes infinitely. The option `--num-tries` can be used to control after how many bugs SQLancer terminates. Alternatively, the option `--timeout-seconds` can be used to specify the maximum duration that SQLancer is allowed to run.

If you launch SQLancer without parameters, available options and commands are displayed. Note that general options that are supported by all DBMS-testing implementations (e.g., `--num-threads`) need to precede the name of DBMS to be tested (e.g., `sqlite3`). Options that are supported only for specific DBMS (e.g., `--test-rtree` for SQLite3), or options for which each testing implementation provides different values (e.g. `--oracle NoREC`) need to go after the DBMS name.

# Testing Approaches

| Approach                                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pivoted Query Synthesis (PQS)                        | PQS is the first technique that we designed and implemented. It randomly selects a row, called a pivot row, for which a query is generated that is guaranteed to fetch the row. If the row is not contained in the result set, a bug has been detected. It is fully described [here](https://arxiv.org/abs/2001.04174). PQS is the most powerful technique, but also requires more implementation effort than the other two techniques. It is currently unmaintained. |
| Non-optimizing Reference Engine Construction (NoREC) | NoREC aims to find optimization bugs. It is described [here](https://www.manuelrigger.at/preprints/NoREC.pdf). It translates a query that is potentially optimized by the DBMS to one for which hardly any optimizations are applicable, and compares the two result sets. A mismatch between the result sets indicates a bug in the DBMS.                                                                                                                                                                                                        |
| Ternary Logic Partitioning (TLP)                     | TLP partitions a query into three partitioning queries, whose results are composed and compare to the original query's result set. A mismatch in the result sets indicates a bug in the DBMS. In contrast to NoREC and PQS, it can detect bugs in advanced features such as aggregate functions.                                                                                                                                                                                                                                                  |
| Cardinality Estimation Restriction Testing (CERT)    | CERT aims to find performance issues through unexpected estimated cardinalities, which represent the estimated number of returned rows. It is described [here](https://arxiv.org/abs/2306.00355). It derives a query to a more restrict query, whose estimated cardinality should be no more than that for the original query. An violation indicates a potential performance issue. CERT supports TiDB, CockroachDB, and MySQL. |

# Generation Approaches
| Approach | Description |
|----------|-------------|
| Random Generation | Random generation is the default test case generation approach in SQLancer. First, random tables are generated. Then queries are randomly generated based on the schemas of the tables. |
| Query Plan Guidance (QPG) | QPG is a test case generation method guided by query plan coverage. Given a database state, we mutate it after no new unique query plans have been observed by randomly-generated queries on the database state aiming to cover more unique query plans for exposing more logics of DBMSs. This approach is enabled by option `--qpg-enable` and now supports TLP and NoREC oracles for SQLite, CockroachDB, TiDB, and Materialize. |

Please find the `.bib` entries [here](docs/PAPERS.md).

# Supported DBMS

Since SQL dialects differ widely, each DBMS to be tested requires a separate implementation.

| DBMS                         | Status      | Expression Generation        | Description                                                                                                                                                                                     |
|------------------------------|-------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SQLite                       | Working     | Untyped                      | This implementation is currently affected by a significant performance regression that still needs to be investigated                                                                           |
| MySQL                        | Working     | Untyped                      | Running this implementation likely uncovers additional, unreported bugs.                                                                                                                        |
| PostgreSQL                   | Working     | Typed                        |                                                                                                                                                                                                 |
| Citus (PostgreSQL Extension) | Working | Typed                        | This implementation extends the PostgreSQL implementation of SQLancer, and was contributed by the Citus team.                                                                                   |
| MariaDB                      | Preliminary | Untyped                      | The implementation of this DBMS is very preliminary, since we stopped extending it after all but one of our bug reports were addressed. Running it likely uncovers additional, unreported bugs. |
| CockroachDB                  | Working     | Typed                        |                                                                                                                                                                                                 |
| TiDB                         | Working     | Untyped                      |                                                                                                                                                                                                 |
| DuckDB                       | Working     | Untyped, Generic             |                                                                                                                                                                                                 |
| ClickHouse                   | Preliminary | Untyped, Generic             | Implementing the different table engines was not convenient, which is why only a very preliminary implementation exists.                                                                        |
| TDEngine                     | Removed     | Untyped                      | We removed the TDEngine implementation since all but one of our bug reports were still unaddressed five months after we reported them.                                                          |
| OceanBase                    | Working     | Untyped                      |                                                                                                                                                                                                 |
| YugabyteDB                   | Working     | Typed (YSQL), Untyped (YCQL) | YSQL implementation based on Postgres code. YCQL implementation is primitive for now and uses Cassandra JDBC driver as a proxy interface.                                                       |
| Databend                     | Working     | Typed                      |  |
| QuestDB                      | Working     | Untyped, Generic                      | The implementation of QuestDB is still WIP, current version covers very basic data types, operations and SQL keywords. |
| CnosDB                       |Working      | Typed                        | The implementation of CnosDB currently uses Restful API.                                                                                                                                        |
| Materialize                  |Working      | Typed                        |
| Apache Doris                 | Preliminary | Typed                   | This is a preliminary implementation, which only contains the common logic of Doris. We have found some errors through it, and hope to improve it in the future.
| Presto                       | Preliminary | Typed                   | This is a preliminary implementation, only basic types supported. 



# Using SQLancer

## Logs

SQLancer stores logs in the `target/logs` subdirectory. By default, the option `--log-each-select` is enabled, which results in every SQL statement that is sent to the DBMS being logged. The corresponding file names are postfixed with `-cur.log`. In addition, if SQLancer detects a logic bug, it creates a file with the extension `.log`, in which the statements to reproduce the bug are logged.

## Reducing a Bug

After finding a bug, it is useful to produce a minimal test case before reporting the bug, to save the DBMS developers' time and effort. For many test cases, [C-Reduce](https://embed.cs.utah.edu/creduce/) does a great job.

## Found Bugs

We would appreciate it if you mention SQLancer when you report bugs found by it. We would also be excited to know if you are using SQLancer to find bugs, or if you have extended it to test another DBMS (also if you do not plan to contribute it to this project). SQLancer has found over 400 bugs in widely-used DBMS, which are listed [here](https://www.manuelrigger.at/dbms-bugs/).


# Community

We have created a [Slack workspace](https://join.slack.com/t/sqlancer/shared_invite/zt-eozrcao4-ieG29w1LNaBDMF7OB_~ACg) to discuss SQLancer, and DBMS testing in general. SQLancer's official Twitter handle is [@sqlancer_dbms](https://twitter.com/sqlancer_dbms).

# FAQ

## I am running SQLancer on the latest version of a supported DBMS. Is it expected that SQLancer prints many AssertionErrors?

In many cases, SQLancer does not support the latest version of a DBMS. You can check the [`.github/workflows/main.yml`](https://github.com/sqlancer/sqlancer/blob/master/.github/workflows/main.yml) file to determine which version we use in our CI tests, which corresponds to the currently supported version of that DBMS. SQLancer should print only an `AssertionError` and produce a corresponding log file, if it has identified a bug. To upgrade SQLancer to support a new DBMS version, either two options are advisable: (1) the generators can be updated to no longer generate certain patterns that might cause errors (e.g., which might be the case if a keyword or option is no longer supported) or (2) the newly-appearing errors can be added as [expected errors](https://github.com/sqlancer/sqlancer/blob/354d591cfcd37fa1de85ec77ec933d5d975e947a/src/sqlancer/common/query/ExpectedErrors.java) so that SQLancer ignores them when they appear (e.g., this is useful if some error-inducing patterns cannot easily be avoided).

Another reason for many failures on a supported version could be that error messages are printed in a non-English locale (which would then be visible in the stack trace). In such a case, try setting the DBMS' locale to English (e.g., see the [PostgreSQL homepage](https://www.postgresql.org/docs/current/locale.html)).

## When starting SQLancer, I get an error such as "database 'test' does not exist". How can I run SQLancer without this error?

For some DBMSs, SQLancer expects that a database "test" exists, which it then uses as an initial database to connect to. If you have not yet created such a database, you can use a command such as `CREATE DATABASE test` to create this database (e.g., see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-createdatabase.html)).

# Additional Documentation

* [Contributing to SQLancer](CONTRIBUTING.md)
* [Papers and .bib entries](docs/PAPERS.md)

# Releases

Official release are available on:
* [GitHub](https://github.com/sqlancer/sqlancer/releases)
* [Maven Central](https://search.maven.org/artifact/com.sqlancer/sqlancer)
* [DockerHub](https://hub.docker.com/r/mrigger/sqlancer)

# Additional Resources

* A talk on Ternary Logic Partitioning (TLP) and SQLancer is available on [YouTube](https://www.youtube.com/watch?v=Np46NQ6lqP8).
* An (older) Pivoted Query Synthesis (PQS) talk is available on [YouTube](https://www.youtube.com/watch?v=yzENTaWe7qg).
* PingCAP has implemented PQS, NoREC, and TLP in a tool called [go-sqlancer](https://github.com/chaos-mesh/go-sqlancer).
* More information on our DBMS testing efforts and the bugs we found is available [here](https://www.manuelrigger.at/dbms-bugs/).
