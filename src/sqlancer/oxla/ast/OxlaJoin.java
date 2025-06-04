package sqlancer.oxla.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.oxla.OxlaToStringVisitor;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaTable;

public class OxlaJoin implements OxlaExpression, Join<OxlaExpression, OxlaTable, OxlaColumn> {
    public enum JoinType {
        INNER("INNER"),
        LEFT_OUTER("LEFT OUTER"),
        LEFT("LEFT"),
        RIGHT("RIGHT"),
        RIGHT_OUTER("RIGHT OUTER"),
        FULL_OUTER("FULL OUTER"),
        FULL("FULL"),
        OUTER("OUTER"),
        CROSS("CROSS");

        private final String textRepresentation;

        JoinType(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String toString() {
            return textRepresentation;
        }

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public final OxlaExpression leftTable;
    public final OxlaExpression rightTable;
    public JoinType type;
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

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
