package sqlancer.citus.gen;

import java.util.Set;

import sqlancer.postgres.gen.PostgresCommon;

public class CitusCommon extends PostgresCommon {

    private CitusCommon() {
    }

    public static void addCitusErrors(Set<String> errors) {
        errors.add("recursive CTEs are not supported in distributed queries");
        errors.add("could not run distributed query with GROUPING SETS, CUBE, or ROLLUP");
        errors.add("Subqueries in HAVING cannot refer to outer query");
        errors.add("non-IMMUTABLE functions are not allowed in the RETURNING clause");
        errors.add("functions used in UPDATE queries on distributed tables must not be VOLATILE");
        errors.add("STABLE functions used in UPDATE queries cannot be called with column references");
        // should be fixed now
        // errors.add("failed to evaluate partition key in insert");
        errors.add("functions used in the WHERE clause of modification queries on distributed tables must not be VOLATILE");
        errors.add("cannot perform an INSERT with NULL in the partition column");
        errors.add("cannot execute ADD CONSTRAINT command with other subcommands");
        errors.add("cannot execute ALTER TABLE command involving partition column");
        errors.add("could not run distributed query with FOR UPDATE/SHARE commands");
        // TODO: remove once fixed
        // errors.add("unrecognized node type: 127");
        // TODO: SQLancer error
        errors.add("not a foreign key or check constraint");
        errors.add("cannot perform an INSERT without a partition column value");
        // TODO: remove once fixed
        errors.add("failed to find conversion function from unknown to text");
        // ERROR: cannot create foreign key constraint
        // Detail: SET NULL or SET DEFAULT is not supported in ON DELETE operation when distribution key is included in the foreign key constraint
        errors.add("cannot create foreign key constraint");
        // ERROR: cannot pushdown the subquery
        // Detail: Complex subqueries and CTEs cannot be in the outer part of the outer join
        errors.add("cannot pushdown the subquery");
        // TODO: remove after PostgreSQL 13 upgrade
        errors.add("unrecognized configuration parameter \"enable_hashagg_disk\"");
        // TODO: remove after PostgreSQL 13 upgrade
        errors.add("unrecognized configuration parameter \"enable_groupingsets_hash_disk\"");
        errors.add("is not a regular, foreign or partitioned table");
        errors.add("must be a distributed table or a reference table");
        // Citus restrictions on SELECT queries
        errors.add("complex joins are only supported when all distributed tables are co-located and joined on their distribution columns");
        errors.add("complex joins are only supported when all distributed tables are joined on their distribution columns with equal operator");
        errors.add("cannot perform distributed planning on this query");
        // errors.add("the query contains a join that requires repartitioning");
        // SQLancer error
        errors.add("non-integer constant in GROUP BY");
        // SQLancer error
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        // SQLancer error
        errors.add("GROUP BY position");
    }
    
}