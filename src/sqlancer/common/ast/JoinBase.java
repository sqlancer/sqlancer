package sqlancer.common.ast;

import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;

public abstract class JoinBase<T extends Expression<?>> {
    public final T tableReference;
    public T onClause;
    public JoinType type;

    public T leftTable;
    public T rightTable;

    protected JoinBase(T tableReference, T onClause, JoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
        this.leftTable = null;
        this.rightTable = null;
    }

    protected JoinBase(T leftTable, T rightTable, T onClause, JoinType type) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.onClause = onClause;
        this.type = type;
        this.tableReference = null;
    }

    protected JoinBase(T tableReference, T onClause, JoinType type, T leftTable, T rightTable) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    protected JoinBase(JoinType type, T onClause) {
        this.tableReference = null;
        this.onClause = onClause;
        this.type = type;
        this.leftTable = null;
        this.rightTable = null;
    }

    public T getTableReference() {
        return tableReference;
    }

    public T getOnClause() {
        return onClause;
    }

    public void setOnClause(T onClause) {
        this.onClause = onClause;
    }

    public JoinType getType() {
        return type;
    }

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, CROSS, JoinType, NATURAL, STRAIGHT, OUTER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER,
        LEFT_ANTI, RIGHT_ANTI;

        private static final JoinType[] DATAFUSION_TYPES = {INNER};
        private static final JoinType[] MYSQL_TYPES = {INNER, LEFT, RIGHT};
        private static final JoinType[] POSTGRES_TYPES = {INNER, LEFT, RIGHT, FULL};

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static JoinType getRandomForDatabase(String dbType) {
            JoinType[] allowedTypes;
            switch(dbType) {
            case "DATAFUSION":
                allowedTypes = DATAFUSION_TYPES;
                break;
            case "MYSQL":
                allowedTypes = MYSQL_TYPES;
                break;
            case "POSTGRES":
                allowedTypes = POSTGRES_TYPES;
                break;
            default:
                allowedTypes = values();
            }
            return Randomly.fromOptions(allowedTypes);
        }

        public static JoinType getRandomExcept(JoinType... exclude) {
            JoinType[] values = Arrays.stream(values()).filter(m -> !Arrays.asList(exclude).contains(m))
                    .toArray(JoinType[]::new);
            return Randomly.fromOptions(values);
        }

    }
}
