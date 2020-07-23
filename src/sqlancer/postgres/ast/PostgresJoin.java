package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresCTE;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;

public class PostgresJoin implements PostgresExpression {

    public enum PostgresJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static PostgresJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final PostgresTableReference tableReference;
    private final PostgresExpression onClause;
    private final PostgresJoinType type;

    public static class PostgresTableReference implements PostgresExpression {

        private final PostgresExpression tableReference;

        public PostgresTableReference(PostgresCTE cte) {
            this.tableReference = cte;
        }

        public PostgresTableReference(PostgresTable table) {
            this.tableReference = new PostgresFromTable(table, Randomly.getBoolean());
        }

        public PostgresExpression getTableReference() {
            return tableReference;
        }

    }

    public PostgresJoin(PostgresTableReference tableReference, PostgresExpression onClause, PostgresJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public PostgresTableReference getTableReference() {
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
