package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresJoin implements PostgresExpression {

    public enum PostgresJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static PostgresJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final PostgresExpression tableReference;
    private final PostgresExpression onClause;
    private final PostgresJoinType type;

    public PostgresJoin(PostgresExpression tableReference, PostgresExpression onClause, PostgresJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public PostgresExpression getTableReference() {
        return tableReference;
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
