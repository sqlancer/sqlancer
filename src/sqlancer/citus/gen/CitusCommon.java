package sqlancer.citus.gen;

import java.util.Collection;
import java.util.Set;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.gen.PostgresCommon;

public class CitusCommon extends PostgresCommon {

    public static void addCitusErrors(Collection<String> errors) {
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
        // ERROR: cannot create foreign key constraint
        // Detail: SET NULL or SET DEFAULT is not supported in ON DELETE operation when distribution key is included in
        // the foreign key constraint
        errors.add("cannot create foreign key constraint");

        // Citus restrictions on SELECT queries
        errors.add(
                "complex joins are only supported when all distributed tables are co-located and joined on their distribution columns");
        errors.add(
                "complex joins are only supported when all distributed tables are joined on their distribution columns with equal operator");
        errors.add("cannot perform distributed planning on this query");
        errors.add("cannot pushdown the subquery");
        // Check for whether repartition joins are enabled is made during query generation
        // errors.add("the query contains a join that requires repartitioning");

        // SQLancer errors
        errors.add("non-integer constant in GROUP BY");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY position");
        errors.add("not a foreign key or check constraint");

        // current Citus errors to be removed once fixed
        errors.add("unrecognized node type: 127");
        errors.add("failed to find conversion function from unknown to text");
        errors.add("failed to evaluate partition key in insert");
        errors.add("cannot perform an INSERT without a partition column value");
        errors.add("cannot perform an INSERT with NULL in the partition column");
        errors.add("ERROR: LIMIT must not be negative");
        errors.add("value too long for type");

        // current errors to be removed once upgraded to PostgreSQL 13?
        errors.add("unrecognized configuration parameter \"enable_hashagg_disk\"");
        errors.add("unrecognized configuration parameter \"enable_groupingsets_hash_disk\"");
    }

    public static void addTableConstraint(StringBuilder sb, PostgresTable table, PostgresGlobalState globalState,
            Set<String> errors) {
        addTableConstraint(sb, table, globalState, Randomly.fromOptions(TableConstraints.values()), errors);
        CitusCommon.addCitusErrors(errors);
    }

}
