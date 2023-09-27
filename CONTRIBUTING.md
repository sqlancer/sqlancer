# Development

## Working with Eclipse

Developing SQLancer using Eclipse is expected to work well. You can import SQLancer with a single step:

```
File -> Import -> Existing Maven Projects -> Select the SQLancer directory as root directory -> Finish
```
If you do not find an option to import Maven projects, you might need to install the [M2Eclipse plugin](https://www.eclipse.org/m2e/).


## Implementing Support for a New DBMS

The DuckDB implementation provides a good template for a new implementation. The `DuckDBProvider` class is the central class that manages the creation of the databases and executes the selected test oracles. Try to copy its structure for the new DBMS that you want to implement, and start by generate databases (without implementing a test oracle). As part of this, you will also need to implement the equivalent of `DuckDBSchema`, which represents the database schema of the generated database. After you can successfully generate databases, the next step is to generate one of the test oracles. For example, you might want to implement NoREC (see `DuckDBNoRECOracle` or `DuckDBQueryPartitioningWhereTester` for TLP). As part of this, you must also implement a random expression generator (see `DuckDBExpressionGenerator`) and a visitor to derive the textual representation of an expression (see `DuckDBToStringVisitor`).

Please consider the following suggestions when creating a  PR to contribute a new DBMS:
* Ensure that `mvn verify -DskipTests=true` does not result in style violations.
* Add a [CI test](https://github.com/sqlancer/sqlancer/blob/master/.github/workflows/main.yml) to ensure that future changes to SQLancer are unlikely to break the newly-supported DBMS. It is reasonable to do this in a follow-up PR—please indicate whether you plan to do so in the PR description.
* Add the DBMS' name to the [check_names.py](https://github.com/sqlancer/sqlancer/blob/master/src/check_names.py) script, which ensures adherence to a common prefix in the Java classes.
* Add the DBMS' name to the [README.md](https://github.com/sqlancer/sqlancer/blob/master/README.md#supported-dbms) file.
* It would be easier to review multiple smaller PRs, than one PR that contains the complete implementation. Consider contributing parts of your implementation as you work on their implementation.

### Expected Errors

Most statements have an [ExpectedError](https://github.com/sqlancer/sqlancer/blob/aa0c0eccba4eefa75bfd518f608c9222c692c11d/src/sqlancer/common/query/ExpectedErrors.java) object associated with them. This object essentially contains a list of errors, one of which the database system might return if it cannot successfully execute the statement. These errors are typically added through a trial-and-error process while considering various tradeoffs. For example, consider the [DuckDBInsertGenerator](https://github.com/sqlancer/sqlancer/blob/aa0c0eccba4eefa75bfd518f608c9222c692c11d/src/sqlancer/duckdb/gen/DuckDBInsertGenerator.java#L38) class, whose expected errors are specified in [DuckDBErrors](https://github.com/sqlancer/sqlancer/blob/aa0c0eccba4eefa75bfd518f608c9222c692c11d/src/sqlancer/duckdb/DuckDBErrors.java#L90). When implementing such a generator, the list of expected errors might first be empty. When running the generator for the first time, you might receive an error such as "create unique index, table contains duplicate data", indicating that creating the index failed due to duplicate data. In principle, this error could be avoided by first checking whether the column contains any duplicate values. However, checking this would be expensive and error-prone (e.g., consider string similarity, which might depend on collations); thus, the obvious choice would be to add this string to the list of expected errors, and run the generator again to check for any other expected errors. In other cases, errors might be best addressed through improvements in the generators. For example, it is typically straightforward to generate syntactically-valid statements, which is why syntax errors should not be ignored. This approach is effective in uncovering internal errors; rather than ignoring them as an expected error, report them, and see [Unfixed Bugs](#unfixed-bugs) below.

### Bailing Out While Generating a Statement

In some cases, it might be undesirable or even impossible to generate a specific statement type. For example, consider that SQLancer tries to execute a `DROP TABLE` statement (e.g., see [TiDBDropTableGenerator](https://github.com/sqlancer/sqlancer/blob/30948f34acc2354d6be18a70bdeeebff1e73fa48/src/sqlancer/tidb/gen/TiDBDropTableGenerator.java)), but the database contains only a single table. Dropping the table would result in all subsequent attempts to insert data or query it to fail. Thus, in such a case, it might be more efficient to "bail out" by abandoning the current attempt to generate the statement. This can be achieved by throwing a `IgnoreMeException`. Unlike for other exceptions, SQLancer silently continues execution rather than reporting this exception to the user.


### Typed vs. Untyped Expression Generation

Each DBMS implementation provides an expression generator used, for example, to generate expressions used in `WHERE` clauses. We found that DBMS can be roughly classified into "permissive" ones, which apply implicit type conversions when needed and "strict" ones, which provide only few implicit conversions and output an error when the type is unexpected. For example, consider the following test case:

```sql
CREATE TABLE t0(c0 TEXT);
INSERT INTO t0 VALUES ('1');
SELECT * FROM t0 WHERE c0;
```

If the test case is executed using MySQL, which is a permissive DBMS, the `SELECT` fetches a single row, since the content of the `c0` value is interpreted as a boolean. If the test case is executed using PostgreSQL, which is a strict DBMS, the `SELECT` is not accepted as a valid query, and PostgreSQL outputs an error `"argument of WHERE must be type boolean"`.  The implementation of the expression generator depends on whether we are dealing with a permissive or a strict DBMS. Since SQLancer's main goal is to find logic bugs, we want to generate as many valid queries as possible.

For a permissive DBMS, implementing the expression generator is easier, since the expression generator does not need to care about the type of the expression, since the DBMS will apply any necessary conversions implicitly. For MySQL, the main `generateExpression` method thus does not accept any type as an argument (see [MySQLExpressionGenerator](https://github.com/sqlancer/sqlancer/blob/86647df8aa2dd8d167b5c3ce3297290f5b0b2bcd/src/sqlancer/mysql/gen/MySQLExpressionGenerator.java#L54)). This method can  be called when a expression is required for, for example, a `WHERE` clause. In principle, this approach can also be used for strict DBMS, by adding errors such as `argument of WHERE must be type boolean` to the list of expected errors. However, using such an "untyped" expression generator for a strict DBMS will result in many semantically invalid queries being generated.

For a strict DBMS, the better approach is typically to attempt to generate expressions of the expected type. For PostgreSQL, the expression generator thus expects an additional type argument (see [PostgreSQLExpressionGenerator](https://github.com/sqlancer/sqlancer/blob/86647df8aa2dd8d167b5c3ce3297290f5b0b2bcd/src/sqlancer/postgres/gen/PostgresExpressionGenerator.java#L251)). This type is propagated recursively. For example, if we require a predicate for the `WHERE` clause, we pass boolean as a type. The expression generator then calls a method `generateBooleanExpression` that attempts to produce a boolean expression, by, for example, generating a comparison (e.g., `<=`). For the comparison's operands, a random type is then selected and propagated. For example, if an integer type is selected, then `generateExpression` is called with this type once for the left operand, and once for the right operand. Note that this process does not guarantee that the expression will indeed have the expected type. It might happen, for example, that the expression generator attempts to produce an integer value, but that it produces a double value instead, namely when an integer overflow occurs, which, depending on the DBMS, implicitly converts the result to a floating-point value.

### Unfixed Bugs

Often, some bugs are fixed only after an extended period, meaning that SQLancer will repeatedly report the same bug. In such cases, it might be possible to avoid generating the problematic pattern, or adding an expected error with the internal error message. Rather than, for example, commenting out the code with the bug-inducing pattern, a pattern implemented by the [TiDBBugs class](https://github.com/sqlancer/sqlancer/blob/4c20a94b3ad2c037e1a66c0b637184f8c20faa7e/src/sqlancer/tidb/TiDBBugs.java) should be applied. The core idea is to use a public, static flag for each issue, which is set to true as long as the issue persists (e.g., see [bug35652](https://github.com/sqlancer/sqlancer/blob/4c20a94b3ad2c037e1a66c0b637184f8c20faa7e/src/sqlancer/tidb/TiDBBugs.java#L55)). The work-around code is then executed—or the problematic pattern should not be generated—if the flag is set to true (e.g., [an expected error is added for bug35652](https://github.com/sqlancer/sqlancer/blob/59564d818d991d54b32fa5a79c9f733799c090f2/src/sqlancer/tidb/TiDBErrors.java#L47)). This makes it easy to later on identify and remove all such work-around code once the issue has been fixed.

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

We use [GitHub Actions](https://github.com/sqlancer/sqlancer/blob/master/.github/workflows/main.yml) to automatically check PRs.


## Testing

As part of the GitHub Actions check, we use smoke testing by running SQLancer on each supported DBMS for some minutes, to test that nothing is obviously broken. For DBMS for which all bugs have been fixed, we verify that SQLancer cannot find any further bugs (i.e., the return code is zero).

In addition, we use [unit tests](https://github.com/sqlancer/sqlancer/tree/master/test/sqlancer) to test SQLancer's core functionality, such as random string and number generation as well as option passing. When fixing a bug, add a unit test, if it is easily possible.

You can run the tests using the following command:

```
mvn test
```

Note that per default, the smoke testing is performed only for embedded DBMS (e.g., DuckDB and SQLite). To run smoke tests also for the other DBMS, you need to set environment variables. For example, you can run the MySQL smoke testing (and no other tests) using the following command:

```
MYSQL_AVAILABLE=true mvn -Dtest=TestMySQL test
```

For up-to-date testing commands, check out the `.github/workflows/main.yml` file.

## Reviewing

Reviewing is an effective way of improving code quality. Everyone is welcome to review any PRs. Currently, all PRs are reviewed at least by the main contributor, @mrigger. Contributions by @mrigger are currently not (necessarily) reviewed, which is not ideal. If you are willing to regularly and timely review PRs, indicate so in the SQLancer Slack workspace.

## Naming Conventions

Each class specific to a DBMS is prefixed by the DBMS name. For example, each class specific to SQLite is prefixed by `SQLite3`. The naming convention is [automatically checked](src/check_names.py).

## Commit History

Please pay attention to good commit messages (in particular subject lines). As basic guidelines, we recommend a blog post on [How to Write a Git Commit Message](https://chris.beams.io/posts/git-commit/) written Chris Beams, which provides 7 useful rules. Implement at least the following of those rules:
1. Capitalize the subject line. For example, write "**R**efactor the handling of indexes" rather than "**r**efactor the handling of indexes".
2. Do not end the subject line with a period. For example, write "Refactor the handling of indexes" rather than "Refactor the handling of indexes.".
3. Use the imperative mood in the subject line. For example, write "Refactor the handling of indexes" rather than "Refactoring" or "Refactor**ed** the handling of indexes".

Please also pay attention to a clean commit history. Rather than merging with the main branch, use `git rebase` to rebase your commits on the main branch. Sometimes, it might happen that you discover an issue only after having already created a commit, for example, when an issue is found by `mvn verify` in the CI checks. Do not introduce a separate commit for such issues. If the issue was introduced by the last commit, you can fix the issue, and use `git commit --amend` to change the latest commit. If the change was introduced by one of the previous commits, you can use `git rebase -i` to change the respective commit. If you already have a number of such commits, you can use `git squash` to "collapse" multiple commits into one. For more information, you might want to read [How (and Why!) to Keep Your Git Commit History Clean](https://about.gitlab.com/blog/2018/06/07/keeping-git-commit-history-clean/) written by Kushal Pandya.
