package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.Join;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaTable;

public class OxlaJoin implements OxlaExpression, Join<OxlaExpression, OxlaTable, OxlaColumn> {
    public enum JoinType {INNER, LEFT_OUTER, LEFT, RIGHT, RIGHT_OUTER, FULL, OUTER, CROSS, NATURAL}

    public final OxlaExpression leftTable;
    public final OxlaExpression rightTable;
    public JoinType type = JoinType.FULL;
    public OxlaExpression onClause;

    public OxlaJoin(OxlaExpression leftTable, OxlaExpression rightTable, JoinType type, OxlaExpression onClause) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.type = type;
        this.onClause = onClause;
    }

    @Override
    public void setOnClause(OxlaExpression onClause) {
        throw new AssertionError("stupid interface requirement; use the variable instead.");
    }
}
