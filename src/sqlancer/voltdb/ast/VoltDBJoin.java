package sqlancer.voltdb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;

// One "<type> JOIN table ON condition" in a FROM clause; it joins onto the FROM list before it.
public class VoltDBJoin implements VoltDBExpression, Join<VoltDBExpression, VoltDBTable, VoltDBColumn> {

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    private final VoltDBTableReference table;
    private final JoinType joinType;
    private VoltDBExpression onCondition;

    public VoltDBJoin(VoltDBTableReference table, JoinType joinType, VoltDBExpression onCondition) {
        this.table = table;
        this.joinType = joinType;
        this.onCondition = onCondition;
    }

    public VoltDBTableReference getTable() {
        return table;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public VoltDBExpression getOnCondition() {
        return onCondition;
    }

    @Override
    public void setOnClause(VoltDBExpression onClause) {
        this.onCondition = onClause;
    }

}
