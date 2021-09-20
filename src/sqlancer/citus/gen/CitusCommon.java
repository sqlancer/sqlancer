package sqlancer.citus.gen;

import sqlancer.citus.CitusBugs;
import sqlancer.common.query.ExpectedErrors;

public final class CitusCommon {

    private CitusCommon() {
    }

    public static void addCitusErrors(ExpectedErrors errors) {
        // not supported by Citus
        errors.add("failed to evaluate partition key in insert");
        errors.add("cannot perform an INSERT without a partition column value");
        errors.add("cannot perform an INSERT with NULL in the partition column");
        errors.add("recursive CTEs are not supported in distributed queries");
        errors.add("could not run distributed query with GROUPING SETS, CUBE, or ROLLUP");
        errors.add("Subqueries in HAVING cannot refer to outer query");
        errors.add("non-IMMUTABLE functions are not allowed in the RETURNING clause");
        errors.add("functions used in UPDATE queries on distributed tables must not be VOLATILE");
        errors.add("STABLE functions used in UPDATE queries cannot be called with column references");
        errors.add(
                "functions used in the WHERE clause of modification queries on distributed tables must not be VOLATILE");
        errors.add("cannot execute ADD CONSTRAINT command with other subcommands");
        errors.add("cannot execute ALTER TABLE command involving partition column");
        errors.add("could not run distributed query with FOR UPDATE/SHARE commands");
        errors.add("is not a regular, foreign or partitioned table");
        errors.add("must be a distributed table or a reference table");
        errors.add("creating unique indexes on non-partition columns is currently unsupported");
        errors.add("modifying the partition value of rows is not allowed");
        errors.add("creating unique indexes on non-partition columns is currently unsupported");
        errors.add("Distributed relations must not use GENERATED ... AS IDENTITY");
        errors.add("cannot drop multiple distributed objects in a single command");
        errors.add("is not distributed");
        errors.add("cannot create constraint on");
        errors.add("cannot create foreign key constraint"); // SET NULL or SET DEFAULT is not supported in ON DELETE
                                                            // operation when distribution key is included in the
                                                            // foreign key constraint
        errors.add("cannot modify views over distributed tables");

        // not supported by Citus (restrictions on SELECT queries)
        errors.add(
                "complex joins are only supported when all distributed tables are co-located and joined on their distribution columns");
        errors.add(
                "complex joins are only supported when all distributed tables are joined on their distribution columns with equal operator");
        errors.add("cannot perform distributed planning on this query");
        errors.add("cannot pushdown the subquery");
        // see https://github.com/sqlancer/sqlancer/issues/215
        errors.add("direct joins between distributed and local tables are not supported");
        errors.add("unlogged columnar tables are not supported");
        errors.add("UPDATE and CTID scans not supported for ColumnarScan");
        errors.add("indexes not supported for columnar tables");

        // current errors in Citus (to be removed once fixed)
        if (CitusBugs.bug3957) {
            errors.add("unrecognized node type: 127");
        }
        if (CitusBugs.bug3980 || CitusBugs.bug3987 || CitusBugs.bug4019) {
            errors.add("syntax error at or near");
        }
        if (CitusBugs.bug3982) {
            errors.add("failed to find conversion function from unknown to text");
            errors.add("invalid input syntax for");
        }
        if (CitusBugs.bug4013) {
            errors.add("ERROR: LIMIT must not be negative");
        }
        if (CitusBugs.bug3981) {
            errors.add("value too long for type");
        }
        if (CitusBugs.bug4014) {
            errors.add("is ambiguous");
        }
        if (CitusBugs.bug4079) {
            errors.add("aggregate function calls cannot be nested");
        }
    }

}
