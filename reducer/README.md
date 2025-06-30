# SimpleReducer Documentation

## Overview

SimpleReducer is a lightweight SQL statement reduction tool designed to minimize bug-triggering SQL statement sets. It implements a partition-based reduction algorithm similar to creduce, which systematically removes unnecessary SQL statements while preserving the bug-triggering behavior.

SimpleReducer supports two main types of error reduction:

1. **Exception Reduction**

   - Description: Reduces SQL statements that trigger specific database exceptions

   - Matching Strategy: Intelligent error message matching with similarity checking

   - Use Case: Database crashes, syntax errors, constraint violations, etc.

2. **Oracle Reduction**

   - Supported Oracles: `NoREC`, `TLPWHERE`

   - Use Case: Logic bugs, optimization errors, semantic inconsistencies, etc.

## Usage

#### Step 1: Generate Serialized Context

First, run SQLancer with the `--save-reducer-context` option to generate a serialized context file when a bug is found:

```bash
java -jar target/sqlancer-2.0.0.jar --save-reducer-context sqlite3 --oracle NoREC
```

This will create a serialized context file like `logs/sqlite3/database0.ser` when a bug is detected.

#### Step 2: Run SimpleReducer

```bash
# Compile
javac -cp target/sqlancer-2.0.0.jar -d . reducer/SimpleReducer.java

# Run
java -cp target/sqlancer-2.0.0.jar:. reducer.SimpleReducer /path/to/xxx.ser /path/to/reduce.log
```

## Test Results

### Test 1: NoREC Oracle Reduction

**Command:**

```bash
java -jar target/sqlancer-2.0.0.jar --random-seed 1697613568309 --save-reducer-context sqlite3 --oracle NoREC
java -cp target/sqlancer-2.0.0.jar:. reducer.SimpleReducer /path/to/database0.ser /path/to/database0-reduce.log
```

**Output:**

```
Initial size: 79 statements
Target error: [SQLITE_CORRUPT]  The database disk image is malformed (database disk image is malformed)

Pass 1 (partition=2): no change
Pass 1 (partition=4): 79 -> 60 statements
Pass 2 (partition=3): 60 -> 40 statements
...
Pass 15 (partition=4): no change

Reduction completed:
Final size: 4 statements (94.9% reduction)
Results saved to: .../database0-reduce.log
Reduction completed successfully!
```

**Final Reduced Statements:**

```sql
CREATE VIRTUAL TABLE vt0 USING fts4(c0 UNINDEXED, prefix=426, order=DESC);
INSERT OR IGNORE INTO vt0(c0) VALUES (0.7581753911019898), (0x7c8cbe06), ('-707350067');
INSERT OR ABORT INTO vt0 VALUES (0.5674108139086818);
INSERT INTO vt0(vt0) VALUES('integrity-check');
```

### Test 2: TestSimpleReducer Unit Tests

Contains tests for three error types: `Exception`, `NoREC Oracle `, `TLPWHERE Oracle` . The original bug URL:

- https://www.sqlite.org/src/tktview?name=771fe61761
- https://www.sqlite.org/src/tktview?name=a7debbe0ad
- https://github.com/cwida/duckdb/issues/590

**Command:**

```bash
# Compile test
javac -cp target/sqlancer-2.0.0.jar -d . reducer/SimpleReducer.java reducer/TestSimpleReducer.java

# Run test
java -cp target/sqlancer-2.0.0.jar:. reducer.TestSimpleReducer
```

**Output:**

```
=== SimpleReducer Test Suite ===

--- Exception Reduction ---
Initial size: 20 statements
Target error: [SQLITE_CORRUPT]  The database disk image is malformed (database disk image is malformed)

Pass 1 (partition=2): no change
Pass 1 (partition=4): 20 -> 15 statements
Pass 2 (partition=3): 15 -> 10 statements
Pass 3 (partition=2): no change
Pass 3 (partition=4): 10 -> 8 statements
Pass 4 (partition=3): 8 -> 6 statements
Pass 5 (partition=2): no change
Pass 5 (partition=4): 6 -> 5 statements
Pass 6 (partition=3): 5 -> 4 statements
Pass 7 (partition=2): no change
Pass 7 (partition=4): no change

Reduction completed:
Final size: 4 statements (80.0% reduction)
Final reduced statements:
  CREATE VIRTUAL TABLE vt0 USING fts4(c0 UNINDEXED, prefix=426, order=DESC);
  INSERT OR IGNORE INTO vt0(c0) VALUES (0.7581753911019898);
  INSERT OR ABORT INTO vt0 VALUES (0.5674108139086818);
  INSERT INTO vt0(vt0) VALUES('integrity-check');

--- NoREC Oracle Reduction ---
Initial size: 21 statements
Target oracle: NoREC

Pass 1 (partition=2): 21 -> 11 statements
Pass 2 (partition=2): 11 -> 6 statements
Pass 3 (partition=2): 6 -> 3 statements
Pass 4 (partition=2): no change
Pass 4 (partition=3): no change

Reduction completed:
Final size: 3 statements (85.7% reduction)
Final reduced statements:
  CREATE TABLE t0 (c0 INTEGER, c1 TEXT, c2 REAL);
  INSERT INTO t0(c0) VALUES('');
  CREATE VIEW v2(c0, c1) AS SELECT 'B' COLLATE NOCASE, 'a' FROM t0 ORDER BY t0.c0;

--- TLP WHERE Oracle Reduction ---
Initial size: 6 statements
Target oracle: TLPWhere

Pass 1 (partition=2): 6 -> 3 statements
Pass 2 (partition=2): 3 -> 2 statements
Pass 3 (partition=2): 2 -> 1 statements

Reduction completed:
Final size: 1 statements (83.3% reduction)
Final reduced statements:
  INSERT INTO t0(c0) VALUES (DATE '2000-01-04');

=== All tests completed successfully ===
```