# SQLancer

![SQLancer](media/logo/png/sqlancer_logo_logo_pos.png)

SQLancer (Synthesized Query Lancer) is a tool to automatically test Database Management Systems (DBMS) in order to find logic bugs in their implementation. We refer to logic bugs as those bugs that cause the DBMS to fetch an incorrect result set (e.g., by omitting a record).

SQLancer operates in the following two phases:

1. Database generation: The goal of this phase is to create a populated database, and stress the DBMS to increase the probability of causing an inconsistent database state that could be detected subsequently. First, random tables are created. Then, randomly SQL statements are chosen to generate, modify, and delete data. Also other statements, such as those to create indexes as well as views and to set DBMS-specific options are sent to the DBMS.
2. Testing: The goal of this phase is to detect the logic bugs based on the generated database. See Testing Approaches below.

# Getting Started

Requirements:
* Java 11
* [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)
* The DBMS that you want to test (SQLite is an embedded DBMS and is included)

The following commands clone SQLancer, create a JAR, and start SQLancer to fuzz SQLite using Ternary Logic Query Partitioning (TLP):

```
git clone https://github.com/sqlancer/sqlancer
cd sqlancer
mvn package
cd target
java -jar SQLancer-0.0.1-SNAPSHOT.jar --num_threads 16 --num_tries 5 --max_expression_depth 3 --num_queries 100000 --max_num_inserts 30 sqlite3 --oracle query_partitioning
```

If the execution prints progress information every five seconds, then the tool works as expected. Note that SQLancer might find bugs in SQLite. Before reporting these, be sure to check that they can still be reproduced when using the latest development version.

If you launch SQLancer without parameters, available options and commands are displayed.

# Potential Commercialization

Due to the significant interest that we have received, we are considering to commercialize our bug-finding efforts. If you represent a company and would be interested in a bug-finding service, please approach us ([Manuel Rigger](mailto:manuel.rigger@inf.ethz.ch) and [Zhendong Su](mailto:zhendong.su@inf.ethz.ch)) with your expectations and requirements for such a service.

# Research Prototype

This project should at this stage still be seen as a research prototype. We believe that the tool is not ready to be used. However, we have received many requests by companies, organizations, and individual developers, which is why we decided to prematurely release the tool. Expect errors, incompatibilities, lack of documentation, and insufficient code quality. That being said, we are working hard to address these issues and enhance SQLancer to become a production-quality piece of software. We welcome any issue reports, extension requests, and code contributions.

# Testing Approaches

| Approach                                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pivoted Query Synthesis (PQS)                        | PQS is the first technique that we designed and implemented. It randomly selects a row, called a pivot row, for which a query is generated that is guaranteed to fetch the row. If the row is not contained in the result set, a bug has been detected. It is fully described [here](https://arxiv.org/abs/2001.04174). PQS is the most powerful technique, but also requires more implementation effort than the other two techniques. It is currently unmaintained. |
| Non-optimizing Reference Engine Construction (NoREC) | NoREC aims to find optimization bugs. It is described [here](https://www.manuelrigger.at/preprints/NoREC.pdf). It translates a query that is potentially optimized by the DBMS to one for which hardly any optimizations are applicable, and compares the two result sets. A mismatch between the result sets indicates a bug in the DBMS.                                                                                                                                                                                                        |
| Ternary Logic Partitioning (TLP)                     | TLP partitions a query into three partitioning queries, whose results are composed and compare to the original query's result set. A mismatch in the result sets indicates a bug in the DBMS. In contrast to NoREC and PQS, it can detect bugs in advanced features such as aggregate functions.                                                                                                                                                                                                                                                  |

# Supported DBMS

Since SQL dialects differ widely, each DBMS to be tested requires a separate implementation.

| DBMS        | Status      | Expression Generation | Description                                                                                                                                                                                     |
|-------------|-------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SQLite      | Working     | Untyped               | This implementation is currently affected by a significant performance regression that still needs to be investigated                                                                           |
| MySQL       | Working     | Untyped               | Running this implementation likely uncovers additional, unreported bugs.                                                                                                                        |
| PostgreSQL  | Working     | Typed                 |                                                                                                                                                                                                 |
| MariaDB     | Preliminary | Untyped               | The implementation of this DBMS is very preliminary, since we stopped extending it after all but one of our bug reports were addressed. Running it likely uncovers additional, unreported bugs. |
| CockroachDB | Working     | Typed                 |                                                                                                                                                                                                 |
| TiDB        | Working     | Untyped               |                                                                                                                                                                                                 |
| DuckDB      | Working     | Untyped, Generic      | DuckDB currently [does not provide a Maven artifact](https://github.com/cwida/duckdb/issues/649), which is why its JDBC driver needs to be manually added.                                      |
| ClickHouse  | Preliminary | Untyped, Generic      | Implementing the different table engines was not convenient, which is why only a very preliminary implementation exists.                                                                        |
| TDEngine    | Removed     | Untyped               | We removed the TDEngine implementation since all but one of our bug reports were still unaddressed five months after we reported them.                                                          |


# Continuous Integration and Test Suite

To improve and maintain SQLancer's code quality, we use several tools:
* The [Eclipse code formatter](https://code.revelc.net/formatter-maven-plugin/), to ensure a consistent formatting (Run `mvn formatter:format` to format all files).
* [Checkstyle](https://checkstyle.sourceforge.io/), to enforce a consistent coding standard.
* [PMD](https://pmd.github.io/), which finds programming flaws using static analysis.
* [SpotBugs](https://spotbugs.github.io/), which also uses static analysis to find bugs and programming flaws.

You can run them using the following command:

```
mvn verify
```

We plan to soon add a [CI](https://github.com/sqlancer/sqlancer/issues/2) to automatically check PRs. Subsequently, we also plan to add smoke testing for each DBMS to test that the respective testing implementation is not obviously broken, see [here](https://github.com/sqlancer/sqlancer/issues/3).

SQLancer does currently not have a test suite. We found that bugs in SQLancer are quickly found and easy to debug when testing the DBMS. The PQS implementation had a test suite, which was removed in commit 36ede0c0c68b3856e03ef5ba802a7c2575bb3f12.

# Using SQLancer

## Logs

SQLancer stores logs in the `target/logs` subdirectory. By default, the option `--log-each-select` is enabled, which results in every SQL statement that is sent to the DBMS being logged. The corresponding file names are postfixed with `-cur.log`. In addition, if SQLancer detects a logic bug, it creates a file with the extension `.log`, in which the statements to reproduce the bug are logged.

## Reducing a Bug

After finding a bug, it is useful to produce a minimal test case before reporting the bug, to save the DBMS developers' time and effort. For many test cases, [C-Reduce](https://embed.cs.utah.edu/creduce/) does a great job. In addition, we have been working on a SQL-specific reducer, which we plan to release soon.

## Found Bugs

We would appreciate it if you mention SQLancer when you report bugs found by it. We would also be excited to know if you are using SQLancer to find bugs, or if you have extended it to test another DBMS (also if you do not plan to contribute it to this project). SQLancer has found over 400 bugs in widely-used DBMS, which are listed [here](https://www.manuelrigger.at/dbms-bugs/).

# Extending and Improving SQLancer

## Implementing Support for a New DBMS

The DuckDB implementation provides a good template for a new implementation. The `DuckDBProvider` class is the central class that manages the creation of the databases and executes the selected test oracles. Try to copy its structure for the new DBMS that you want to implement, and start by generate databases (without implementing a test oracle). As part of this, you will also need to implement the equivalent of `DuckDBSchema`, which represents the database schema of the generated database. After you can successfully generate databases, the next step is to generate one of the test oracles. For example, you might want to implement NoREC (see `DuckDBNoRECOracle` or `DuckDBQueryPartitioningWhereTester` for TLP). As part of this, you must also implement a random expression generator (see `DuckDBExpressionGenerator`) and a visitor to derive the textual representation of an expression (see `DuckDBToStringVisitor`).

## Working with Eclipse

Developing SQLancer using Eclipse is expected to work well. You can import SQLancer with a single step:

```
File -> Import -> Existing Maven Projects -> Select the SQLancer directory as root directory -> Finish
```
If you do not find an option to import Maven projects, you might need to install the [M2Eclipse plugin](https://www.eclipse.org/m2e/).


# Community

We have created a [Slack workspace](https://join.slack.com/t/sqlancer/shared_invite/zt-eozrcao4-ieG29w1LNaBDMF7OB_~ACg) to discuss SQLancer, and DBMS testing in general. SQLancer's official Twitter handle is [@sqlancer_dbms](https://twitter.com/sqlancer_dbms).

# Additional Resources

* An (older) Pivoted Query Synthesis (PQS) talk is available on [YouTube](https://www.youtube.com/watch?v=yzENTaWe7qg).
* PingCAP has implemented PQS in a tool called [wreck-it](https://github.com/chaos-mesh/wreck-it).
* More information on our DBMS testing efforts and the bugs we found is available [here](https://www.manuelrigger.at/dbms-bugs/).
