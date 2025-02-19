package sqlancer.sqlite3.ast;

import static sqlancer.sqlite3.ast.SQLite3AffinityHelper.applyAffinities;

import java.util.List;
import java.util.Optional;

import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.ast.SQLite3AffinityHelper.ConstantTuple;

public class SQLite3InOperation implements SQLite3Expression {

    private final SQLite3Expression left;
    private List<SQLite3Expression> rightExpressionList;
    private SQLite3Expression rightSelect;

    public SQLite3InOperation(SQLite3Expression left, List<SQLite3Expression> right) {
        this.left = left;
        this.rightExpressionList = right;
    }

    public SQLite3InOperation(SQLite3Expression left, SQLite3Expression select) {
        this.left = left;
        this.rightSelect = select;
    }

    public SQLite3Expression getLeft() {
        return left;
    }

    public List<SQLite3Expression> getRightExpressionList() {
        return rightExpressionList;
    }

    public SQLite3Expression getRightSelect() {
        return rightSelect;
    }

    @Override
    // The collating sequence used for expressions of the form "x IN (y, z, ...)" is
    // the collating sequence of x.
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        if (left.getExplicitCollateSequence() != null) {
            return left.getExplicitCollateSequence();
        } else {
            return null;
        }
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        // TODO query as right hand side is not implemented
        if (left.getExpectedValue() == null) {
            return null;
        }
        if (rightExpressionList.isEmpty()) {
            return SQLite3Constant.createFalse();
        } else if (left.getExpectedValue().isNull()) {
            return SQLite3Constant.createNullConstant();
        } else {
            boolean containsNull = false;
            for (SQLite3Expression expr : getRightExpressionList()) {
                if (expr.getExpectedValue() == null) {
                    return null; // TODO: we can still compute something if the value is already contained
                }
                SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate = getExplicitCollateSequence();
                if (collate == null) {
                    collate = left.getImplicitCollateSequence();
                }
                if (collate == null) {
                    collate = SQLite3Schema.SQLite3Column.SQLite3CollateSequence.BINARY;
                }
                ConstantTuple convertedConstants = applyAffinities(left.getAffinity(), SQLite3TypeAffinity.NONE,
                        left.getExpectedValue(), expr.getExpectedValue());
                SQLite3Constant equals = left.getExpectedValue().applyEquals(convertedConstants.right, collate);
                Optional<Boolean> isEquals = SQLite3Cast.isTrue(equals);
                if (isEquals.isPresent() && isEquals.get()) {
                    return SQLite3Constant.createTrue();
                } else if (!isEquals.isPresent()) {
                    containsNull = true;
                }
            }
            if (containsNull) {
                return SQLite3Constant.createNullConstant();
            } else {
                return SQLite3Constant.createFalse();
            }
        }
    }
}
