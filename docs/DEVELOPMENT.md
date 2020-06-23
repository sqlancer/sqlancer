# Development

## Working with Eclipse

Developing SQLancer using Eclipse is expected to work well. You can import SQLancer with a single step:

```
File -> Import -> Existing Maven Projects -> Select the SQLancer directory as root directory -> Finish
```
If you do not find an option to import Maven projects, you might need to install the [M2Eclipse plugin](https://www.eclipse.org/m2e/).


## Implementing Support for a New DBMS

The DuckDB implementation provides a good template for a new implementation. The `DuckDBProvider` class is the central class that manages the creation of the databases and executes the selected test oracles. Try to copy its structure for the new DBMS that you want to implement, and start by generate databases (without implementing a test oracle). As part of this, you will also need to implement the equivalent of `DuckDBSchema`, which represents the database schema of the generated database. After you can successfully generate databases, the next step is to generate one of the test oracles. For example, you might want to implement NoREC (see `DuckDBNoRECOracle` or `DuckDBQueryPartitioningWhereTester` for TLP). As part of this, you must also implement a random expression generator (see `DuckDBExpressionGenerator`) and a visitor to derive the textual representation of an expression (see `DuckDBToStringVisitor`).

## Options

SQLancer uses [JCommander](https://jcommander.org/) for handling options. The `MainOptions` class contains options that are expected to be supported by all DBMS-testing implementations. Furthermore, each `*Provider` class provides a method to return an additional set of supported options.

An option can include lowercase alphanumeric characters, and hyphens. The format of the options is checked by a unit test.

## Continuous Integration and Test Suite

To improve and maintain SQLancer's code quality, we use multiple tools:
* The [Eclipse code formatter](https://code.revelc.net/formatter-maven-plugin/), to ensure a consistent formatting (Run `mvn formatter:format` to format all files).
* [Checkstyle](https://checkstyle.sourceforge.io/), to enforce a consistent coding standard.
* [PMD](https://pmd.github.io/), which finds programming flaws using static analysis.
* [SpotBugs](https://spotbugs.github.io/), which also uses static analysis to find bugs and programming flaws.

You can run them using the following command:

```
mvn verify
```

We use [Travis-CI](https://travis-ci.com/) to automatically check PRs.


## Testing

We found that bugs in SQLancer are quickly found and easy to debug when testing the DBMS. However, it would still be preferable to automatically check that SQLancer still executes as expected. To this end, we would like to add smoke testing for each DBMS to test that the respective testing implementation is not obviously broken, see [here](https://github.com/sqlancer/sqlancer/issues/3).
