package sqlancer.voltdb;

import java.sql.SQLException;

import sqlancer.common.query.ExpectedErrors;

/**
 * Errors VoltDB raises for statements that are invalid by design of the generator. Each entry names the reason the
 * generator cannot avoid it, so that no whole error class is suppressed without one.
 */
public final class VoltDBErrors {

    /**
     * Rejections caused by a schema change on another thread, which {@link VoltDBConnection} retries so they are never
     * reported. VoltDB runs one catalog update (DDL statement) at a time and rejects a concurrent one; and it aborts an
     * ad hoc statement that was planned before a catalog update committed. Both are rejected before they run.
     */
    private static final String[] RETRYABLE = { "while another one is in progress",
            "was planned against a catalog version which does not match",
            // the same, raised by the planner itself once a catalog update has replaced it (PlannerTool.planSql), and
            // for DDL planned against a configuration that is no longer live; and the ad hoc planner's queue being
            // full. txnid2 (TxnId2Utils) retries the same rejections.
            "The planner is outdated", "was no longer live", "Ad Hoc Planner task queue is full",
            // the same while a node fails or rejoins (run-sqlancer.sh --kill-interval): VoltZK's catalog update
            // blocker refuses DDL, before it starts, while a rejoin, leader promotion, transaction repair or an
            // interrupted catalog update is in progress ("Can't do catalog update(@AdHoc) while ...")
            "Can't do catalog update(@AdHoc) while",
            // another connection's catalog update (@UpdateCore) is still running; its blocker can be gone when the
            // node that held it failed, so a DDL statement gets this far and is refused before it stages anything
            // (UpdateApplicationBase). Seen once node 0 was among the nodes killed (milestone 15).
            "@UpdateCore in progress",
            // the same while a node rejoins during an in-service upgrade (run-sqlancer.sh --upgrade-from), before the
            // DDL statement stages anything; and its other form, refused while the upgraded node is still rejoining
            // (lab build #43, the upgrade run)
            "Can not update catalog while catalog update is in progress",
            "Can not update catalog while node rejoin is in progress" };

    /**
     * Errors a node failure causes while nodes are killed and rejoined during the run (--node-failures). A statement in
     * flight when a partition's leader moves is dropped, and may or may not have committed; a system request waiting on
     * the dead node times out. {@link VoltDBConnection} skips the statement, check or database that hit one.
     */
    private static final String[] NODE_FAILURE = { "Transaction dropped due to change in mastership",
            "OPS request hit sixty second timeout", "Transaction being restarted due to fault recovery",
            // a transaction a failure interrupted, or whose completion it caught (txnid2 lists both as transient)
            "Transaction Interrupted", "transaction completion is in progress",
            // a catalog update (DDL) whose verification step was waiting on the dead node
            "failed, connection lost",
            // a DDL statement that one of the errors above interrupted can still commit after its client was told it
            // failed, so a later CREATE of the same name collides with an object the provider never saw
            "object name already exists",
            // a call in flight on the connection to a node that was killed (with --servers, the other connections
            // carry the next calls)
            "was lost before a response was received" };

    private VoltDBErrors() {
    }

    public static boolean isNodeFailure(SQLException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        for (String r : NODE_FAILURE) {
            if (message.contains(r)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRetryable(SQLException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        for (String r : RETRYABLE) {
            if (message.contains(r)) {
                return true;
            }
        }
        return false;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        // VoltDB does not support a join whose WHERE clause folds to a filter on none of the joined tables; a
        // predicate that references a column can still fold to a constant, e.g. (FALSE AND t.c) = FALSE
        errors.add("Join with filters that do not depend on joined tables is not supported");
        // VoltDB plans a join of partitioned tables only when it can see the equality of their partitioning columns.
        // The generator joins them on that equality alone, but an outer join of a replicated table whose ON condition
        // folds to FALSE, with a WHERE clause on that table, can take the equality away; the shapes that do are many
        errors.add("This query is not plannable");
        // ENG-30508: the same rejected filter, when it is an IN with a DECIMAL constant on the left, fails with an
        // internal error instead
        errors.add("NUMERIC constant value type must match a FLOAT, DECIMAL, or integral column, not null");
        // run-time arithmetic errors: random operands divide by zero and overflow, and the two NoREC queries may
        // evaluate an expression on different rows, so only one of them may hit the error
        errors.add("Attempted to divide"); // integer or DECIMAL division by zero
        // the same, in constants the planner folds (data exception: ...), and MOD by zero at run time (SQL ERROR ...)
        errors.add("division by zero");
        errors.add("Invalid result value"); // FLOAT result is infinite or NaN
        errors.add("will overflow BigInt storage"); // integer +, -, * out of range
        errors.add("causing overflow/underflow"); // DECIMAL out of range
        errors.add("out of range for the destination type"); // a huge DECIMAL or FLOAT result cast to an integer
        // DECIMAL out of range in an expression of constants, which the planner folds; the message quotes a
        // truncated value, e.g. "30145.999" for -99999999999999999999999999.999999999999 + -30146
        errors.add("Numeric conversion error to type DECIMAL");
        // the same where the folded value has more digits than DECIMAL's precision allows, e.g. a 26-digit DECIMAL
        // constant times a 9-digit integer
        errors.add("digits of precision");
        // ENG-30522: FLOAT overflow to infinity in an expression of constants, which the parser folds, fails with an
        // internal NumberFormatException instead of "Invalid result value"
        errors.add("Character I is neither a decimal digit number");
        // milestone 19: a JSON function's document is a VARCHAR column or a generated document, and most column values
        // are not JSON
        errors.add("Invalid JSON");
        // DATEADD with a random interval can leave TIMESTAMP's range
        errors.add("interval is too large for DATEADD");
        // DATEADD and TO_TIMESTAMP inputs or results outside TIMESTAMP's years 1583 to 9999 ("would produce a value
        // outside of the supported range", "Input to SQL function DATEADD is outside of the supported range"), and
        // TO_TIMESTAMP of a value beyond TIMESTAMP's microseconds
        errors.add("outside of the supported range (years 1583 to 9999");
        errors.add("would overflow TIMESTAMP data type");
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        // a generated key can repeat an existing primary key or unique value
        errors.add("CONSTRAINT VIOLATION");
        // keeping a view up to date evaluates its WHERE clause on the new row, which can overflow or divide by zero
        addExpressionErrors(errors);
    }

    public static void addUpdateErrors(ExpectedErrors errors) {
        // a random new value can repeat a primary key or unique value, or be NULL in a NOT NULL column; VoltDB checks
        // constraints row by row, so even SET k = k + 1 on a unique k fails once one row's new value meets the next
        errors.add("CONSTRAINT VIOLATION");
        // a value does not fit the column: a constant at planning time, or a computed value when it is stored
        errors.add("is too wide");
        errors.add("exceeds the size of the");
        errors.add("overflows/underflows");
        // a constant with a fraction, such as (0 - 0.42E0), set into an integer column is rejected when planned; not
        // the same message for a DATE column, which is ENG-30561
        for (String type : new String[] { "tinyint", "smallint", "integer", "bigint" }) {
            errors.add("has an invalid format for a constant " + type + " value");
        }
    }

    public static void addViewErrors(ExpectedErrors errors) {
        addExpressionErrors(errors);
        // a WHERE clause that folds to FALSE makes the planner add LIMIT 0 (ENG-19957, won't fix), which a view rejects
        errors.add("with a LIMIT or OFFSET clause is not supported");
    }

    public static void addIndexErrors(ExpectedErrors errors) {
        // VoltDB cannot add a UNIQUE index (an ASSUMEUNIQUE one it can) to a table that already holds rows
        errors.add("is not empty");
    }

}
