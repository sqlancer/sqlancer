package sqlancer.sqlite3.ast;

import sqlancer.IgnoreMeException;

public final class SQLite3AffinityHelper {

    private SQLite3AffinityHelper() {
        // Utility class
    }

    static class ConstantTuple {
        SQLite3Constant left;
        SQLite3Constant right;

        ConstantTuple(SQLite3Constant left, SQLite3Constant right) {
            this.left = left;
            this.right = right;
        }

    }

    public static ConstantTuple applyAffinities(SQLite3TypeAffinity leftAffinity, SQLite3TypeAffinity rightAffinity,
            SQLite3Constant leftBeforeAffinity, SQLite3Constant rightBeforeAffinity) {
        // If one operand has INTEGER, REAL or NUMERIC affinity and the other operand
        // has TEXT or BLOB or no affinity then NUMERIC affinity is applied to other
        // operand.
        SQLite3Constant left = leftBeforeAffinity;
        SQLite3Constant right = rightBeforeAffinity;
        if (leftAffinity.isNumeric() && (rightAffinity == SQLite3TypeAffinity.TEXT
                || rightAffinity == SQLite3TypeAffinity.BLOB || rightAffinity == SQLite3TypeAffinity.NONE)) {
            right = right.applyNumericAffinity();
            assert right != null;
        } else if (rightAffinity.isNumeric() && (leftAffinity == SQLite3TypeAffinity.TEXT
                || leftAffinity == SQLite3TypeAffinity.BLOB || leftAffinity == SQLite3TypeAffinity.NONE)) {
            left = left.applyNumericAffinity();
            assert left != null;
        }

        // If one operand has TEXT affinity and the other has no affinity, then TEXT
        // affinity is applied to the other operand.
        if (leftAffinity == SQLite3TypeAffinity.TEXT && rightAffinity == SQLite3TypeAffinity.NONE) {
            right = right.applyTextAffinity();
            if (right == null) {
                throw new IgnoreMeException();
            }
        } else if (rightAffinity == SQLite3TypeAffinity.TEXT && leftAffinity == SQLite3TypeAffinity.NONE) {
            left = left.applyTextAffinity();
            if (left == null) {
                throw new IgnoreMeException();
            }
        }
        return new ConstantTuple(left, right);
    }
}
