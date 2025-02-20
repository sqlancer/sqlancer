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

        private static final JoinType[] DATAFUSION_TYPES = { INNER };
        private static final JoinType[] DATABEND_TYPES = { INNER, NATURAL, LEFT, RIGHT };
        private static final JoinType[] H2_TYPES = { INNER, CROSS, NATURAL, LEFT, RIGHT };
        private static final JoinType[] PRESTO_TYPES = { INNER, LEFT, RIGHT };
        private static final JoinType[] POSTGRES_TYPES = { INNER, LEFT, RIGHT, FULL, CROSS };
        private static final JoinType[] SQLITE3_TYPES = { INNER, CROSS, OUTER, NATURAL, RIGHT, FULL };
        private static final JoinType[] TIDB_TYPES = { NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS };
        private static final JoinType[] COCKROACHDB_TYPES = { INNER, LEFT, RIGHT, FULL, CROSS, NATURAL };
        private static final JoinType[] DORIS_TYPES = { INNER, STRAIGHT, LEFT, RIGHT };

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static JoinType getRandomForDatabase(String dbType) {
            JoinType[] allowedTypes;
            switch (dbType) {
            case "DATAFUSION":
                allowedTypes = DATAFUSION_TYPES;
                break;
            case "DATABEND":
            case "DUCKDB":
            case "HSQLDB":
                allowedTypes = DATABEND_TYPES;
                break;
            case "H2":
                allowedTypes = H2_TYPES;
                break;
            case "PRESTO":
                allowedTypes = PRESTO_TYPES;
                break;
            case "POSTGRES":
            case "YSQL":
                allowedTypes = POSTGRES_TYPES;
                break;
            case "TIDB":
                allowedTypes = TIDB_TYPES;
                break;
            case "COCKROACHDB":
                allowedTypes = COCKROACHDB_TYPES;
                break;
            case "DORIS":
                allowedTypes = DORIS_TYPES;
                break;
            default:
                allowedTypes = values();
            }
            return Randomly.fromOptions(allowedTypes);
        }

        public static JoinType getRandomExcept(String dbType, JoinType... exclude) {
            JoinType[] allowedTypes;

            switch (dbType) {
            case "POSTGRES":
                allowedTypes = POSTGRES_TYPES;
                break;
            case "TIDB":
                allowedTypes = TIDB_TYPES;
                break;
            case "COCKROACHDB":
                allowedTypes = COCKROACHDB_TYPES;
                break;
            default:
                allowedTypes = values();
            }

            JoinType[] values = Arrays.stream(allowedTypes).filter(m -> !Arrays.asList(exclude).contains(m))
                    .toArray(JoinType[]::new);
            return Randomly.fromOptions(values);
        }

        public static JoinType[] getValues(String dbType) {
            JoinType[] allowedTypes;

            switch (dbType) {
            case "SQLITE3":
                allowedTypes = SQLITE3_TYPES;
                break;
            case "MARIADB":
                allowedTypes = TIDB_TYPES;
                break;
            default:
                allowedTypes = values();
            }
            return allowedTypes;
        }
    }
}
