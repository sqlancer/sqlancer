[![Build Status](https://github.com/sqlancer/sqlancer/workflows/ci/badge.svg)](https://github.com/sqlancer/sqlancer/actions)


![SQLancer](media/logo/png/sqlancer_logo_logo_pos_500.png)

SQLancer is a tool to automatically test Database Management Systems (DBMSs) in order to find bugs in their implementation. That is, it finds bugs in the code of the DBMS implementation, rather than in queries written by the user. SQLancer has found hundreds of bugs in mature and widely-known DBMSs.

SQLancer tackles two essential challenges when automatically testing the DBMSs:
1. **Test input generation**: SQLancer implements approaches for automatically generating SQL statements. It contains various hand-written SQL generators that operate in multiple phases. First, a database schema is created, which refers to a set of tables and their columns. Then, data is inserted into these tables, along with creating various other kinds of database states such as indexes, views, or database-specific options. Finally, queries are generated, which can be validated using one of multiple result validators (also called *test oracles*) that SQLancer provides. Besides the standard approach of creating the statements in an unguided way, SQLancer also supports a test input-generation approach that is feedback-guided and aims to exercise as many unique query plans as possible based on the intuition that doing so would exercise many interesting behaviors in the database system [[ICSE '23]](https://arxiv.org/pdf/2312.17510).
2. **Test oracles**: A key innovation in SQLancer is that it provides ways to find deep kinds of bugs in DBMSs. As a main focus, it can find logic bugs, which are bugs that cause the DBMS to fetch an incorrect result set (e.g., by omitting a record). We have proposed multiple complementary test oracles such as *Ternary Logic Partitioning (TLP)* [[OOPSLA '20]](https://dl.acm.org/doi/pdf/10.1145/3428279), *Non-optimizing Reference Engine Construction (NoREC)* [[ESEC/FSE 2020]](https://arxiv.org/abs/2007.08292), *Pivoted Query Synthesis (PQS)* [[OSDI '20]](https://www.usenix.org/system/files/osdi20-rigger.pdf), *Differential Query Plans (DQP)* [[SIGMOD '24]](https://dl.acm.org/doi/pdf/10.1145/3654991), and *Constant Optimization Driven Database System Testing (CODDTest)* [SIGMOD '25].  It can also find specific categories of performance issues, which refer to cases where a DBMS could reasonably be expected to produce its result more efficiently using a technique called *Cardinality Estimation Restriction Testing (CERT)* [[ICSE '24]](https://arxiv.org/pdf/2306.00355). SQLancer can detect unexpected internal errors (e.g., an error that the database is corrupted) by declaring all potential errors that might be returned by a DBMS for a given query. Finally, SQLancer can find crash bugs, which are bugs that cause the DBMS process to terminate. For this, it uses an implicit test oracle.

**Community.** We have a [Slack workspace](https://join.slack.com/t/sqlancer/shared_invite/zt-eozrcao4-ieG29w1LNaBDMF7OB_~ACg) to discuss SQLancer, and DBMS testing in general. Previously, SQLancer had an account on Twitter/X [@sqlancer_dbms](https://twitter.com/sqlancer_dbms), which is no longer maintained. We have a [blog](https://sqlancer.github.io/posts/), which, as of now, contains only posts by contributors of the [Google Summer of Code project](https://summerofcode.withgoogle.com/archive/2023/organizations/sqlancer).

# Getting Started [[Video Guide]](https://www.youtube.com/watch?v=lcZ6LixPH1Y)

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


# Using SQLancer

**Logs.** SQLancer stores logs in the `target/logs` subdirectory. By default, the option `--log-each-select` is enabled, which results in every SQL statement that is sent to the DBMS being logged. The corresponding file names are postfixed with `-cur.log`. In addition, if SQLancer detects a logic bug, it creates a file with the extension `.log`, in which the statements to reproduce the bug are logged, including only the last query that was executed along with the other statements to set up the database state.

**Reducing bugs.** After finding a bug-inducing test input, the input typically needs to be reduced to be further analyzed, as it might contain many SQL statements that are redundant to reproduce the bug. One option is to do this manually, by removing a statement or feature at a time, replaying the bug-inducing statements, and applying the test oracle (e.g., for test oracles like TLP or NoREC, this would require checking that both queries still produce a different result). This process can be automated using a so-called [delta-debugging approach](https://www.debuggingbook.org/html/DeltaDebugger.html). SQLancer includes an experimental implementation of a delta debugging approach, which can be enabled using `--use-reducer`. In the past, we have successfully used [C-Reduce](https://embed.cs.utah.edu/creduce/), which requires specifying the test oracle in a script that can be executed by C-Reduce.

**Testing the latest DBMS version.** For most DBMSs, SQLancer supports only a previous *release* version. Thus, potential bugs that SQLancer finds could be already fixed in the latest *development* version of the DBMS. If you are not a developer of the DBMS that you are testing, we would like to encourage you to validate that the bug can still be reproduced before reporting it. We would appreciate it if you could mention SQLancer when you report bugs found by it. We would also be excited to hear about your experience using SQLancer or related use cases or extensions.

**Options.** SQLancer provides many options that you can use to customize its behavior. Executing `java -jar sqlancer-*.jar --help` will list them and should print output such as the following:
```
Usage: SQLancer [options] [command] [command options]
  Options:
    --ast-reducer-max-steps
      EXPERIMENTAL Maximum steps the AST-based reducer will do
      Default: -1
    --ast-reducer-max-time
      EXPERIMENTAL Maximum time duration (secs) the statement reducer will do
      Default: -1
    --canonicalize-sql-strings
      Should canonicalize query string (add ';' at the end
      Default: true
    --constant-cache-size
      Specifies the size of the constant cache. This option only takes effect
      when constant caching is enabled
      Default: 100
...
```

**Which SQLancer version to use.** The recommended way to use SQLancer is to use its latest source version on GitHub. Infrequent and irregular official releases are also available on the following platforms:
* [GitHub](https://github.com/sqlancer/sqlancer/releases)
* [Maven Central](https://search.maven.org/artifact/com.sqlancer/sqlancer)
* [DockerHub](https://hub.docker.com/r/mrigger/sqlancer)

**Understanding SQL generation.** To analyze bug-inducing statements, it is helpful to understand the characteristics of SQLancer. First, SQLancer is expected to always generate SQL statements that are syntactically valid for the DBMS under test. Thus, you should never observe any syntax errors. Second, SQLancer might generate statements that are semantically invalid. For example, SQLancer might attempt to insert duplicate values into a column with a `UNIQUE` constraint, as completely avoiding such semantic errors is challenging. Third, any bug reported by SQLancer is expected to be a real bug, except those reported by CERT (as performance issues are not as clearly defined as other kinds of bugs). If you observe any bugs indicated by SQLancer that you do not consider bugs, something is likely wrong with your setup. Finally, related to the aforementioned point, SQLancer is specific to a version of the DBMS, and you can find the version against which we are tested in our [GitHub Actions workflow](https://github.com/sqlancer/sqlancer/blob/documentation/.github/workflows/main.yml). If you are testing against another version, you might observe various false alarms (e.g., caused by syntax errors). While we would always like for SQLancer to be up-to-date with the latest development version of each DBMS, we lack the resources to achieve this.

**Supported DBMSs.** SQLancer requires DBMS-specific code for each DBMS that it supports. As of January 2025, it provides support for Citus, ClickHouse, CnosDB, CockroachDB, Databend, (Apache) DataFusion, (Apache) Doris, DuckDB, H2, HSQLDB, MariaDB, Materialize, MySQL, OceanBase, PostgreSQL, Presto, QuestDB, SQLite3, TiDB, and YugabyteDB. The extent to which the individual DBMSs are supported [differs](https://github.com/sqlancer/sqlancer/blob/documentation-approaches/CONTRIBUTING.md).

# Approaches and Papers

SQLancer has pioneered and includes multiple approaches for DBMS testing, as outlined below in chronological order.

| Technique                                                       | Venue         | Links                                                                                                                                                              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|-----------------------------------------------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pivoted Query Synthesis (PQS)                                   | OSDI 2020     | [Paper](https://www.usenix.org/system/files/osdi20-rigger.pdf) [Video](https://www.youtube.com/watch?v=0aeDyXgzo04 )                                               | PQS is the first technique that we designed and implemented. It randomly selects a row, called a pivot row, for which a query is generated that is guaranteed to fetch the row. If the row is not contained in the result set, a bug has been detected. It is fully described here. PQS effectively detects bugs, but requires more implementation effort than other testing approaches that follow a metamorphic testing or differential testing methodology. Thus, it is currently unmaintained.                                                                                              |
| Non-optimizing Reference Engine Construction (NoREC)            | ESEC/FSE 2020 | [Paper](https://arxiv.org/abs/2007.08292) [Video](https://www.youtube.com/watch?v=4mbzytrWJhQ)                                                                     | NoREC aims to find optimization bugs. It translates a query that is potentially optimized by the DBMS to one for which hardly any optimizations are applicable, and compares the two result sets. A mismatch between the result sets indicates a bug in the DBMS. The approach applies primarily to simple queries with a filter predicate.                                                                                                                                                                                                                                                     |
| Ternary Logic Partitioning (TLP)                                | OOPSLA 2020   | [Paper](https://dl.acm.org/doi/pdf/10.1145/3428279) [Video](https://www.youtube.com/watch?v=FN9OLbGh0VI)                                                           | TLP partitions a query into three partitioning queries, whose results are composed and compared to the original query's result set. A mismatch in the result sets indicates a bug in the DBMS. In contrast to NoREC and PQS, it can detect bugs in advanced features such as aggregate functions. It is among the most widely adopted testing techniques.                                                                                                                                                                                                                                       |
| Query Plan Guidance (QPG)                                       | ICSE 2023     | [Paper](https://arxiv.org/pdf/2312.17510) [Video](https://youtu.be/6EjQ1cKiZJU?si=gh7uoykRqNjl3GXR&t=1820) [Code](https://github.com/sqlancer/sqlancer/issues/641) | QPG is a feedback-guided test case generation approach. It is based on the insights that query plans capture whether interesting behavior is exercised within the DBMS. It works by mutating the database state when no new query plans have been observed after executing a number of queries, expecting that the new state enables new query plans to be triggered. This approach is enabled by option `--qpg-enable` and supports TLP and NoREC oracles for SQLite, CockroachDB, TiDB, and Materialize. It is the only approach that specifically tackles the test input generation problem. |
| Cardinality Estimation Restriction Testing (CERT)               | ICSE 2024     | [Paper](https://arxiv.org/pdf/2306.00355) [Code](https://github.com/sqlancer/sqlancer/issues/822)                                                                  | CERT aims to find performance issues through unexpected estimated cardinalities, which represent the estimated number of returned rows. From a given input query, it derives a more restrictive query, whose estimated cardinality should be no more than that of the original query. A violation indicates a potential performance issue. CERT supports TiDB, CockroachDB, and MySQL. CERT is the only test oracle that is part of SQLancer that was designed to find performance issues.                                                                                                      |
| Differential Query Plans (DQP)                                  | SIGMOD 2024   | [Paper](https://dl.acm.org/doi/pdf/10.1145/3654991) [Video](https://www.youtube.com/watch?v=9Qp7quJfGEk) [Code](https://github.com/sqlancer/sqlancer/issues/918)   | DQP aims to find logic bugs by controlling the execution of different query plans for a given query and validating that they produce a consistent result. DQP supports MySQL, MariaDB, and TiDB.                                                                                                                                                                                                                                                                                                                                                                                                |
| Constant Optimization Driven Database System Testing (CODDTest) | SIGMOD 2025   | [Code](https://github.com/sqlancer/sqlancer/pull/1054)                                                                                                             | CODDTest finds logic bugs in DBMSs, including in advanced features such as subqueries. It is based on the insight that we can assume the database state to be constant for a database session, which then enables us to substitute parts of a query with their results, essentially corresponding to constant folding and constant propagation, which are two traditional compiler optimizations.                                                                                                                                                                                               |

Please find the `.bib` entries [here](docs/PAPERS.md).                                                                               |

# FAQ

**I am running SQLancer on the latest version of a supported DBMS. Is it expected that SQLancer prints many AssertionErrors?** In many cases, SQLancer does not support the latest version of a DBMS. You can check the [`.github/workflows/main.yml`](https://github.com/sqlancer/sqlancer/blob/master/.github/workflows/main.yml) file to determine which version we use in our CI tests, which corresponds to the currently supported version of that DBMS. SQLancer should print only an `AssertionError` and produce a corresponding log file, if it has identified a bug. To upgrade SQLancer to support a new DBMS version, either two options are advisable: (1) the generators can be updated to no longer generate certain patterns that might cause errors (e.g., which might be the case if a keyword or option is no longer supported) or (2) the newly-appearing errors can be added as [expected errors](https://github.com/sqlancer/sqlancer/blob/354d591cfcd37fa1de85ec77ec933d5d975e947a/src/sqlancer/common/query/ExpectedErrors.java) so that SQLancer ignores them when they appear (e.g., this is useful if some error-inducing patterns cannot easily be avoided).

Another reason for many failures on a supported version could be that error messages are printed in a non-English locale (which would then be visible in the stack trace). In such a case, try setting the DBMS' locale to English (e.g., see the [PostgreSQL homepage](https://www.postgresql.org/docs/current/locale.html)).

**When starting SQLancer, I get an error such as "database 'test' does not exist". How can I run SQLancer without this error?** For some DBMSs, SQLancer expects that a database "test" exists, which it then uses as an initial database to connect to. If you have not yet created such a database, you can use a command such as `CREATE DATABASE test` to create this database (e.g., see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-createdatabase.html)).

# Links

Documentation and resources:

* [Contributing to SQLancer](CONTRIBUTING.md)
* [Papers and .bib entries](docs/PAPERS.md)
* More information on our DBMS testing efforts and the bugs we found is available [here](https://www.manuelrigger.at/dbms-bugs/).

Videos:
* [SQLancer Tutorial Playlist](https://www.youtube.com/playlist?list=PLm7ofmclym1E2LwBeSer_AAhzBSxBYDci)
* [SQLancer Talks](https://youtube.com/playlist?list=PLm7ofmclym1E9-AbYy-PkrMfHpB9VdlZJ)

Closely related tools:
* [go-sqlancer](https://github.com/chaos-mesh/go-sqlancer): re-implementation of some of SQLancer's approaches in Go by PingCAP
* [Jepsen](https://github.com/jepsen-io): testing of distributed (database) systems
* [SQLRight](https://github.com/PSU-Security-Universe/sqlright): coverage-guided DBMS fuzzer, also supporting NoREC and TLP
* [SQLsmith](https://github.com/anse1/sqlsmith): random SQL query generator used for fuzzing
* [Squirrel](https://github.com/s3team/Squirrel): coverage-guided DBMS fuzzer
