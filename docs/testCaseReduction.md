# Test Case Reduction
SQLancer generates a large number of statements, but not all of them are relevant to the bug. To automatically reduce the test cases, two reducers were implemented: the statement reducer and the AST-based reducer.

## Statement Reducer
The statement reducer utilizes the delta-debugging technique to remove irrelevant statements. More details of delta-debugging could be found in this paper: [Simplifying and Isolating Failure-Inducing Input](https://www.cs.purdue.edu/homes/xyzhang/fall07/Papers/delta-debugging.pdf). 

Using the statement reducer, SQLancer reduces the set of statements to a minimal subset that reproduces the bug. 

## AST-Based Reducer
The AST-based reducer can shorten a statement by applying AST level transformations, including removing unnecessary clauses, irrelevant elements in a list, simplify complicated expressions and etc. 

The transformations are implemented by [JSQLParser](https://github.com/JSQLParser/JSqlParser), a RDBMS agnostic SQL statement parser that can translate SQL statements into a traversable hierarchy of Java classes. JSQLParser provides support for the SQL standard as well as major SQL dialects. The AST-based reducer works for any SQL dialects that can be parsed by this tool.

## Implementing reproducer
Determining whether a bug persists after reducing statements
is an undecidable task for general transformations.
In practice, reducers use the [reproducer](../src/sqlancer/Reproducer.java) to determine
if a bug remains after statements have been removed or modified.
The reducer's responsibility is to verify if the current state,
formed by the pared-down statements,
continues to yield incorrect results for specific queries.

Different oracles have distinct logic for determination,
meaning a universal reproducer doesn't exist.
Each oracle type needs its own reproducer implementation.
If reproducer is not implemented for specific oracle,
test case reduction is not available while using the oracle.

Oracles for which reproducers have currently been implemented include:
1. for [`SQLite3NoRECOracle`](../src/sqlancer/sqlite3/oracle/SQLite3NoRECOracle.java)
2. for [`TiDBTLPWhereOracle`](../src/sqlancer/tidb/oracle/TiDBTLPWhereOracle.java)

## Using reducers
Test-case reduction is disabled by default. The statement reducer can be enabled by passing `--use-reducer` when starting SQLancer. If you wish to further shorten each statements, you need to additionally pass the `--reduce-ast` parameter so that the AST-based reduction is applied. 

Note: if `--reduce-ast` is set, `--use-reducer` option must be enabled first.

There are also options to define timeout seconds and max steps of reduction for both statement reducer and AST-based reducer.

```
--statement-reducer-max-steps=<steps>
--statement-reducer-max-time=<seconds>
--ast-reducer-max-steps=<steps>
--ast-reducer-max-time=<seconds>
```

## Reduction logs
If test-case reduction is enabled, each time the reducer performs a reduction step successfully,it prints the reduced statements to the log file, overwriting the previous ones.

The log files will be stored in the following format: `logs/<DBMS>/reduce/<database>-reduce.log`. For instance, if the tested DBMS is SQLite3 and the current database is named database0, the log file will be located at `logs/sqlite3/reduce/database0-reduce.log`.
