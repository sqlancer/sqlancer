package sqlancer.cloudberry.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.cloudberry.CloudberryBugs;
import sqlancer.common.query.ExpectedErrors;

/**
 * Expected errors that Apache Cloudberry returns for statements from the reused PostgreSQL generators, mostly due to
 * MPP restrictions (distribution keys, replicated tables, append-optimized / column-oriented / PAX storage) that
 * PostgreSQL does not have.
 */
public final class CloudberryCommon {

    private CloudberryCommon() {
    }

    public static List<String> getCloudberryErrors() {
        List<String> errors = new ArrayList<>();

        // The distribution key must be a subset of every PRIMARY KEY / UNIQUE constraint.
        errors.add("PRIMARY KEY and DISTRIBUTED BY definitions are incompatible");
        errors.add("UNIQUE and DISTRIBUTED BY definitions are incompatible");
        errors.add("must be a subset of the PRIMARY KEY");
        errors.add("must be a subset of the UNIQUE");
        errors.add("distribution policy is not compatible with the table's PRIMARY KEY");
        errors.add("UNIQUE or PRIMARY KEY definitions are incompatible with each other");
        errors.add("Cannot parallelize an UPDATE statement that updates the distribution columns");
        errors.add("must contain all columns in the table's distribution key");
        errors.add("cannot alter type of a column used in a distribution policy");
        errors.add("distribution key");
        // A table with a unique index cannot be distributed RANDOMLY or REPLICATED.
        errors.add("because relation has unique index");

        // A hash distribution key needs a hash operator class.
        errors.add("is not a hash distributable data type");
        errors.add("type does not have a default operator class");
        errors.add("no default operator class");
        errors.add("cannot use column of type");

        // A partitioned table can only be distributed BY or RANDOMLY, and a leaf partition always follows its root.
        errors.add("cannot set the distribution policy of a partitioned table");
        errors.add("can't set the distribution policy of");
        errors.add("relation is not distributed");
        errors.add("gp_distribution_policy");

        // The inherited INSERT/UPDATE generators may target a HASH leaf partition directly with rows that belong to
        // another leaf.
        errors.add("violates partition constraint");

        // Append-optimized / column-oriented / PAX storage restrictions.
        errors.add("append-only");
        errors.add("appendonly");
        errors.add("appendoptimized");
        errors.add("column-oriented");
        errors.add("does not support");
        errors.add("is not supported on appendoptimized relations");
        errors.add("unsupported operation on an appendoptimized relation");
        errors.add("is not supported for appendoptimized relations");
        errors.add("feature not supported on appendoptimized relations");
        errors.add("cannot reindex append-only columnar table");
        errors.add("cannot reindex PAX table");
        errors.add("there is no previously clustered index");
        errors.add("is missing support function");
        // ON CONFLICT is not implemented for PAX tables.
        errors.add("not implemented yet on pax relations");
        // REINDEX ... CONCURRENTLY is not implemented for ao_column tables. Matched by source file, since the bare
        // "not implemented yet" would also hide other unimplemented callbacks, and the line number changes.
        errors.add("not implemented yet (aocsam_handler.c");
        errors.add("and DISTRIBUTED RANDOMLY are incompatible");

        // Cloudberry has no unlogged sequences, so an UNLOGGED table with a serial or identity column is rejected.
        errors.add("unlogged sequences are not supported");

        // UPDATE / DELETE across an inheritance hierarchy whose members have different distribution policy types.
        errors.add("can't split update for inherit table");
        errors.add("ModifyTable mixes distributed and entry-only tables");

        // Unlike PostgreSQL, Cloudberry cannot change the type of a column with a PRIMARY KEY / UNIQUE constraint.
        errors.add("cannot alter column with primary key or unique constraint");

        // Cloudberry's grammar does not accept an unquoted "partition" as a value, e.g. in
        // "SET constraint_exclusion = partition".
        errors.add("syntax error at or near \"partition\"");

        // PostgreSQL partitioning rules, reachable once a partitioned table has leaf partitions.
        errors.add("constraint must be added to child tables too");
        errors.add("cannot remove constraint from only the partitioned table when partitions exist");

        // ORCA uses an INCLUDE (non-key) index column as an Index Cond. Depending on the predicate, the executor
        // rejects it in nodeIndexscan.c or in nbtree.
        if (CloudberryBugs.bug2126) {
            errors.add("bogus index qualification");
            errors.add("btree index keys must be ordered by attribute");
        }

        // Exclusion constraints and foreign keys on distributed tables. An EXCLUDE constraint must also include the
        // distribution key.
        errors.add("exclusion constraints");
        errors.add("EXCLUDE");
        errors.add("exclusion constraint is not compatible with the table's distribution policy");
        errors.add("foreign key constraint");

        // Some storage options (e.g. parallel_workers) are generated from a wider range than the server accepts.
        errors.add("out of bounds for option");

        // Generic MPP planner / executor restrictions.
        errors.add("not supported in Apache Cloudberry");
        errors.add("not supported by Apache Cloudberry");
        errors.add("This query is not currently supported by GPDB");
        // cdbpath.c rejects an UPDATE / DELETE on a replicated table whose plan contains a volatile function, since
        // the replicas could diverge. This does not match pathnode.c's "could not devise a query plan for the given
        // query", which indicates a planner bug.
        errors.add("could not devise a plan");
        errors.add("function cannot execute on a QE slice");
        errors.add("cannot execute on a QE slice");
        errors.add("canceling statement due to statement timeout");

        return errors;
    }

    public static void addCloudberryErrors(ExpectedErrors errors) {
        errors.addAll(getCloudberryErrors());
    }
}
