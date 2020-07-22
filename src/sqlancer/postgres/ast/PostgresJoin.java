package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresCTE;

public class PostgresJoin implements PostgresExpression {

    public enum PostgresJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static PostgresJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final PostgresTable table;
    private final PostgresExpression onClause;
    private final PostgresJoinType type;
    private PostgresCTE CTE = null;

    public PostgresJoin(PostgresTable table, PostgresExpression onClause, PostgresJoinType type) {
        this.table = table;
        this.onClause = onClause;
        this.type = type;
    }

    public PostgresJoin(PostgresCTE CTE, PostgresExpression onClause, PostgresJoinType type) {
        this.CTE = CTE;
        this.onClause = onClause;
        this.type = type;
        this.table = null;
    }

    public PostgresTable getTable() {
        return table;
    }

    public PostgresCTE getCTE() {
        return CTE;
    }

    public boolean joinCTE() {
        return (CTE != null);
    }

    public PostgresExpression getOnClause() {
        return onClause;
    }

    public PostgresJoinType getType() {
        return type;
    }

    @Override
    public PostgresDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public PostgresConstant getExpectedValue() {
        throw new AssertionError();
    }

}
