[![Build Status](https://github.com/sqlancer/sqlancer/workflows/ci/badge.svg)](https://github.com/sqlancer/sqlancer/actions)


![SQLancer](media/logo/png/sqlancer_logo_logo_pos_500.png)

SQLancer is a tool to automatically test Database Management Systems (DBMSs) in order to find bugs in their implementation. That is, it finds bugs in the code of the DBMS implementation, rather than in queries written by the user. SQLancer has found hundreds of bugs in mature and widely-known DBMSs.

SQLancer tackles two essential challenges when automatically testing the DBMSs:
1. **Test input generation**: SQLancer implements approaches for automatically generating SQL statements. It contains various hand-written SQL generators that operate in multiple phases. First, a database schema is created, which refers to a set of tables and their columns. Then, data is inserted into these tables, along with creating various other kinds of database states such as indexes, views, or database-specific options. Finally, queries are generated, which can be validated using one of multiple result validators (also called *test oracles*) that SQLancer provides. Besides the standard approach of creating the statements in an unguided way, SQLancer also supports a test input-generation approach that is feedback-guided and aims to exercise as many unique query plans as possible based on the intuition that doing so would exercise many interesting behaviors in the database system [[ICSE '23]](https://arxiv.org/pdf/2312.17510).
2. **Test oracles**: A key innovation in SQLancer is that it provides ways to find deep kinds of bugs in DBMSs. As a main focus, it can find logic bugs, which are bugs that cause the DBMS to fetch an incorrect result set (e.g., by omitting a record). We have proposed multiple complementary test oracles such as *Ternary Logic Partitioning (TLP)* [[OOPSLA '20]](https://dl.acm.org/doi/pdf/10.1145/3428279), *Non-optimizing Reference Engine Construction (NoREC)* [[ESEC/FSE 2020]](https://arxiv.org/abs/2007.08292), *Pivoted Query Synthesis (PQS)* [[OSDI '20]](https://www.usenix.org/system/files/osdi20-rigger.pdf), *Differential Query Plans (DQP)* [[SIGMOD '24]](https://dl.acm.org/doi/pdf/10.1145/3654991), and *Constant Optimization Driven Database System Testing (CODDTest)* [SIGMOD '25].  It can also find specific categories of performance issues, which refer to cases where a DBMS could reasonably be expected to produce its result more efficiently using a technique called *Cardinality Estimation Restriction Testing (CERT)* [[ICSE '24]](https://arxiv.org/pdf/2306.00355). SQLancer can detect unexpected internal errors (e.g., an error that the database is corrupted) by declaring all potential errors that might be returned by a DBMS for a given query. Finally, SQLancer can find crash bugs, which are bugs that cause the DBMS process to terminate. For this, it uses an implicit test oracle.

# Getting Started

Minimum Requirements:
* Java 11 or above
* [Maven](https://maven.apache.org/)

The following commands clone SQLancer, create a JAR, and start SQLancer to test SQLite using [Non-optimizing Reference Engine Construction (NoREC)](https://arxiv.org/abs/2007.08292):

```
git clone https://github.com/sqlancer/sqlancer
cd sqlancer
mvn package -DskipTests
cd target
java -jar sqlancer-*.jar --num-threads 4 sqlite3 --oracle NoREC
```

**Running and terminating.** If the execution prints progress information every five seconds, then the tool works as expected. The shortcut CTRL+C can be used to terminate SQLancer manually. If SQLancer does not find any bugs, it executes infinitely. The option `--num-tries` can be used to control after how many bugs SQLancer terminates. Alternatively, the option `--timeout-seconds` can be used to specify the maximum duration that SQLancer is allowed to run.

**Parameters.** If you launch SQLancer without parameters, available options and commands are displayed. Note that general options that are supported by all DBMS-testing implementations (e.g., `--num-threads`) need to precede the name of the DBMS to be tested (e.g., `sqlite3`). Options that are supported only for specific DBMS (e.g., `--test-rtree` for SQLite3), or options for which each testing implementation provides different values (e.g. `--oracle NoREC`) need to go after the DBMS name.

**DBMSs.** To run SQLancer on SQLite, it was not necessary to install and set up a DBMS. The reason for this is that embedded DBMSs run in the same process as the application and thus require no separate installation or setup. Embedded DBMSs supported by SQLancer include DuckDB, H2, and SQLite. Their binaries are included as [JAR dependencies](https://github.com/sqlancer/sqlancer/blob/main/pom.xml). Note that any crashes in these systems will also cause a crash in the JVM on which SQLancer runs.


# Testing Approaches

| Approach                                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pivoted Query Synthesis (PQS)                        | PQS is the first technique that we designed and implemented. It randomly selects a row, called a pivot row, for which a query is generated that is guaranteed to fetch the row. If the row is not contained in the result set, a bug has been detected. It is fully described [here](https://arxiv.org/abs/2001.04174). PQS is the most powerful technique, but also requires more implementation effort than the other two techniques. It is currently unmaintained. |
| Non-optimizing Reference Engine Construction (NoREC) | NoREC aims to find optimization bugs. It is described [here](https://www.manuelrigger.at/preprints/NoREC.pdf). It translates a query that is potentially optimized by the DBMS to one for which hardly any optimizations are applicable, and compares the two result sets. A mismatch between the result sets indicates a bug in the DBMS.                                                                                                                                                                                                        |
| Ternary Logic Partitioning (TLP)                     | TLP partitions a query into three partitioning queries, whose results are composed and compare to the original query's result set. A mismatch in the result sets indicates a bug in the DBMS. In contrast to NoREC and PQS, it can detect bugs in advanced features such as aggregate functions.                                                                                                                                                                                                                                                  |
| Cardinality Estimation Restriction Testing (CERT)    | CERT aims to find performance issues through unexpected estimated cardinalities, which represent the estimated number of returned rows. It is described [here](https://arxiv.org/abs/2306.00355). It derives a query to a more restrict query, whose estimated cardinality should be no more than that for the original query. An violation indicates a potential performance issue. CERT supports TiDB, CockroachDB, and MySQL. |
| Differential Query Plans (DQP)                       | DQP aims to find logic bugs in database systems by checking whether the query plans of the same query perform consistently. It is described [here](https://bajinsheng.github.io/assets/pdf/dqp_sigmod24.pdf). DQP supports MySQL, MariaDB, and TiDB.|

# Generation Approaches
| Approach | Description |
|----------|-------------|
| Random Generation | Random generation is the default test case generation approach in SQLancer. First, random tables are generated. Then queries are randomly generated based on the schemas of the tables. |
| Query Plan Guidance (QPG) | QPG is a test case generation method guided by query plan coverage. Given a database state, we mutate it after no new unique query plans have been observed by randomly-generated queries on the database state aiming to cover more unique query plans for exposing more logics of DBMSs. This approach is enabled by option `--qpg-enable` and now supports TLP and NoREC oracles for SQLite, CockroachDB, TiDB, and Materialize. |

Please find the `.bib` entries [here](docs/PAPERS.md).

# Supported DBMS

Since SQL dialects differ widely, each DBMS to be tested requires a separate implementation.

| DBMS                         | Status      | Expression Generation        | Description                                                                                                                                                                                     |
| ---------------------------- | ----------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQLite                       | Working     | Untyped                      | This implementation is currently affected by a significant performance regression that still needs to be investigated                                                                           |
| MySQL                        | Working     | Untyped                      | Running this implementation likely uncovers additional, unreported bugs.                                                                                                                        |
| PostgreSQL                   | Working     | Typed                        |                                                                                                                                                                                                 |
| Citus (PostgreSQL Extension) | Working     | Typed                        | This implementation extends the PostgreSQL implementation of SQLancer, and was contributed by the Citus team.                                                                                   |
| MariaDB                      | Preliminary | Untyped                      | The implementation of this DBMS is very preliminary, since we stopped extending it after all but one of our bug reports were addressed. Running it likely uncovers additional, unreported bugs. |
| CockroachDB                  | Working     | Typed                        |                                                                                                                                                                                                 |
| TiDB                         | Working     | Untyped                      |                                                                                                                                                                                                 |
| DuckDB                       | Working     | Untyped, Generic             |                                                                                                                                                                                                 |
| ClickHouse                   | Preliminary | Untyped, Generic             | Implementing the different table engines was not convenient, which is why only a very preliminary implementation exists.                                                                        |
| TDEngine                     | Removed     | Untyped                      | We removed the TDEngine implementation since all but one of our bug reports were still unaddressed five months after we reported them.                                                          |
| OceanBase                    | Working     | Untyped                      |                                                                                                                                                                                                 |
| YugabyteDB                   | Working     | Typed (YSQL), Untyped (YCQL) | YSQL implementation based on Postgres code. YCQL implementation is primitive for now and uses Cassandra JDBC driver as a proxy interface.                                                       |
| Databend                     | Working     | Typed                        |                                                                                                                                                                                                 |
| QuestDB                      | Working     | Untyped, Generic             | The implementation of QuestDB is still WIP, current version covers very basic data types, operations and SQL keywords.                                                                          |
| CnosDB                       | Working     | Typed                        | The implementation of CnosDB currently uses Restful API.                                                                                                                                        |
| Materialize                  | Working     | Typed                        |                                                                                                                                                                                                 |
| Apache Doris                 | Preliminary | Typed                        | This is a preliminary implementation, which only contains the common logic of Doris. We have found some errors through it, and hope to improve it in the future.                                |
| Presto                       | Preliminary | Typed                        | This is a preliminary implementation, only basic types supported.                                                                                                                               |
| DataFusion                   | Preliminary | Typed                        | Only basic SQL features are supported.                                                                                                                                                          |

## Previously Supported DBMS

Some DBMS were once supported but subsequently removed.

| DBMS       | Pull Request                                    | Description                                                                                                                                              |
| ---------- | ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ArangoDB   | [#915](https://github.com/sqlancer/sqlancer/pull/915) | This implementation was removed because ArangoDB is a NoSQL DBMS, while the majority were SQL DBMSs, which resulted in difficulty refactoring SQLancer.  |
| Cosmos     | [#915](https://github.com/sqlancer/sqlancer/pull/915) | This implementation was removed because Cosmos is a NoSQL DBMS, while the majority were SQL DBMSs, which resulted in difficulty refactoring SQLancer.    |
| MongoDB    | [#915](https://github.com/sqlancer/sqlancer/pull/915) | This implementation was removed because MongoDB is a NoSQL DBMS, while the majority were SQL DBMSs, which resulted in difficulty refactoring SQLancer.   |
| StoneDB    | [#963](https://github.com/sqlancer/sqlancer/pull/963) | This implementation was removed because development of StoneDB stopped.                                                                                  |


# Using SQLancer

## Logs

SQLancer stores logs in the `target/logs` subdirectory. By default, the option `--log-each-select` is enabled, which results in every SQL statement that is sent to the DBMS being logged. The corresponding file names are postfixed with `-cur.log`. In addition, if SQLancer detects a logic bug, it creates a file with the extension `.log`, in which the statements to reproduce the bug are logged.

## Reducing a Bug

After finding a bug, it is useful to produce a minimal test case before reporting the bug, to save the DBMS developers' time and effort. For many test cases, [C-Reduce](https://embed.cs.utah.edu/creduce/) does a great job.

## Found Bugs

For most DBMSs, SQLancer supports only a previous *release* version. Thus, potential bugs that SQLancer finds could be already fixed in the latest *development* version of the DBMS. If you are not a developer of the DBMS that you are testing, we would like to encourage you to validate that the bug can still be reproduced before reporting it.

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
