package sqlancer.voltdb;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;
import sqlancer.voltdb.oracle.VoltDBDerivedTableOracle;
import sqlancer.voltdb.oracle.VoltDBDifferentialOracle;
import sqlancer.voltdb.oracle.VoltDBDurabilityOracle;
import sqlancer.voltdb.oracle.VoltDBGeographyOracle;
import sqlancer.voltdb.oracle.VoltDBLimitOracle;
import sqlancer.voltdb.oracle.VoltDBParameterOracle;
import sqlancer.voltdb.oracle.VoltDBPermissionOracle;
import sqlancer.voltdb.oracle.VoltDBPivotedQuerySynthesisOracle;
import sqlancer.voltdb.oracle.VoltDBPlanOracle;
import sqlancer.voltdb.oracle.VoltDBSchemaChangeOracle;
import sqlancer.voltdb.oracle.VoltDBSetOperationOracle;
import sqlancer.voltdb.oracle.VoltDBSmokeOracle;
import sqlancer.voltdb.oracle.VoltDBStreamOracle;
import sqlancer.voltdb.oracle.VoltDBSubqueryOracle;
import sqlancer.voltdb.oracle.VoltDBTLPAggregateOracle;
import sqlancer.voltdb.oracle.VoltDBTLPOracle;
import sqlancer.voltdb.oracle.VoltDBViewOracle;
import sqlancer.voltdb.oracle.VoltDBWindowOracle;
import sqlancer.voltdb.oracle.VoltDBXdcrOracle;

public enum VoltDBOracleFactory implements OracleFactory<VoltDBGlobalState> {

    /**
     * Reads every table of the database and checks only that the queries succeed. It exercises schema and data
     * generation without checking results.
     */
    SMOKE {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBSmokeOracle(globalState);
        }
    },

    /**
     * Compares the row count of a query that filters with a random predicate in its WHERE clause with the count of an
     * equivalent query that evaluates the predicate per row in its projection, which the planner cannot optimize.
     */
    NOREC {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new NoRECOracle<>(globalState, new VoltDBExpressionGenerator(globalState).setSubqueries(true),
                    expressionErrors());
        }
    },

    /**
     * Checks that a query returns the same rows as the union of the three queries that add WHERE p, WHERE NOT p and
     * WHERE p IS NULL for a random predicate p (Ternary Logic Partitioning).
     */
    TLP_WHERE {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBTLPOracle(globalState, expressionErrors(), VoltDBTLPOracle.Mode.WHERE);
        }
    },

    /**
     * Checks that a SELECT DISTINCT, or a GROUP BY of all its columns, returns the rows of the three queries that add
     * WHERE p, WHERE NOT p and WHERE p IS NULL, without duplicates.
     */
    TLP_DISTINCT {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBTLPOracle(globalState, expressionErrors(), VoltDBTLPOracle.Mode.DISTINCT);
        }
    },

    /**
     * Checks that a GROUP BY query returns the same rows as the three queries that add HAVING p, HAVING NOT p and
     * HAVING p IS NULL, for a random predicate p over the grouping columns and aggregates.
     */
    TLP_HAVING {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBTLPOracle(globalState, expressionErrors(), VoltDBTLPOracle.Mode.HAVING);
        }
    },

    /**
     * Checks that MIN, MAX, SUM and COUNT over all rows equal the same aggregates over the three partitions WHERE p,
     * WHERE NOT p and WHERE p IS NULL, combined.
     */
    TLP_AGGREGATE {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBTLPAggregateOracle(globalState, expressionErrors());
        }
    },

    /**
     * Pivoted Query Synthesis: picks a row of each table, evaluates random predicates on it in Java, and checks that a
     * query whose WHERE clause and ON conditions are rewritten to be TRUE for that row returns it, with the evaluated
     * values of random expressions.
     */
    PQS {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBPivotedQuerySynthesisOracle(globalState, expressionErrors());
        }
    },

    /**
     * Checks that a query returns the same rows when some of its constants are JDBC parameters instead of literals.
     */
    PARAMS {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBParameterOracle(globalState, expressionErrors());
        }
    },

    /**
     * Differential testing: with --mirror, the same query on this cluster and on a second one, normally the last
     * release, that every change is repeated on, must return the same rows.
     */
    DIFF {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBDifferentialOracle(globalState, expressionErrors());
        }
    },

    /**
     * Durability across whole-cluster crashes: with --mirror, every table holds as many rows on this cluster, crashed
     * and recovered, as on the mirror, which never crashes.
     */
    DURABLE {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBDurabilityOracle(globalState);
        }
    },

    /**
     * Streams: each row goes into a stream and a shadow table, and the stream's materialized view must match the same
     * GROUP BY over the shadow table; with --export-target the streams are exported, and run-sqlancer.sh --export
     * checks that every row was.
     */
    STREAMS {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBStreamOracle(globalState);
        }
    },

    /**
     * Permissions, with --security: the users reader (SQLREAD) and writer (SQL) may run what their roles allow and
     * nothing else, and a refused statement changes nothing.
     */
    PERMS {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBPermissionOracle(globalState, expressionErrors());
        }
    },

    /**
     * Differential query plans: the same query planned two ways (an index made unusable, the join order reversed, an
     * index added) returns the same rows.
     */
    PLANS {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBPlanOracle(globalState, expressionErrors());
        }
    },

    /**
     * XDCR convergence: with --xdcr, every table and view holds the same rows on both clusters of the XDCR pair once
     * replication has caught up.
     */
    XDCR {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBXdcrOracle(globalState);
        }
    },

    /**
     * GEOGRAPHY_POINT and GEOGRAPHY against geography computed in Java: coordinates, DISTANCE and CONTAINS, with and
     * without a geography index, on a table of its own. Once per database.
     */
    GEO {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBGeographyOracle(globalState, expressionErrors());
        }
    },

    /**
     * Schema changes mid-run and the plan cache: queries over a table return the same rows from a new plan, from the
     * cached plan, after a change to the table that does not touch what they read (an index, an added column, a wider
     * VARCHAR), and after the change is undone. Once per database.
     */
    SCHEMA {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBSchemaChangeOracle(globalState, expressionErrors());
        }
    },

    /**
     * Ad hoc against stored procedure: a query with literals returns the rows of the same query with parameters as a
     * DDL stored procedure, called with the literals' values; when it reads a partitioned table, half the procedures
     * are single-partition, pinned by an equality on its partitioning column.
     */
    PROCEDURE {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBParameterOracle(globalState, expressionErrors(), true);
        }
    },

    /**
     * Checks that a materialized view holds the rows its defining query returns on the table now, after the inserts,
     * updates and deletes that built the database and sometimes one more change.
     */
    VIEWS {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            ExpectedErrors errors = expressionErrors();
            // a view whose CREATE VIEW failed with an expected error
            errors.add("object not found");
            return new VoltDBViewOracle(globalState, errors);
        }
    },

    /**
     * Checks that LIMIT and OFFSET select the right rows of a query ordered by all its outputs.
     */
    LIMIT {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            ExpectedErrors errors = expressionErrors();
            // an output that is constant in a way the oracle does not see, such as (CASE WHEN ('a') IS NULL ...)
            errors.add("ORDER BY cannot contain constant values");
            return new VoltDBLimitOracle(globalState, errors);
        }
    },

    /**
     * Checks that a query with an IN, ANY or ALL subquery in its WHERE clause returns the same rows as the query with
     * the subquery written with EXISTS.
     */
    SUBQUERY {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBSubqueryOracle(globalState, expressionErrors());
        }
    },

    /**
     * Checks the values of a windowed RANK, DENSE_RANK, ROW_NUMBER, COUNT, SUM, MIN or MAX against the rows of its own
     * query, and that the query returns the rows it would without the window function.
     */
    WINDOW {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBWindowOracle(globalState, expressionErrors());
        }
    },

    /**
     * Checks UNION, INTERSECT and EXCEPT, with and without ALL, against the same operation done in Java on the rows of
     * their operands.
     */
    SETOPS {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBSetOperationOracle(globalState, expressionErrors());
        }
    },

    /**
     * Checks derived tables and WITH: a query against itself wrapped in a derived table, against the same query with
     * its WHERE clause outside a derived table or common table, and a recursive WITH against its closure computed in
     * Java.
     */
    DERIVED {
        @Override
        public TestOracle<VoltDBGlobalState> create(VoltDBGlobalState globalState) {
            return new VoltDBDerivedTableOracle(globalState, expressionErrors());
        }
    };

    private static ExpectedErrors expressionErrors() {
        ExpectedErrors errors = ExpectedErrors.newErrors().build();
        VoltDBErrors.addExpressionErrors(errors);
        return errors;
    }

}
