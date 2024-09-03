package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public class YSQLJoin implements YSQLExpression, Join<YSQLExpression, YSQLTable, YSQLColumn> {

    private final YSQLExpression tableReference;
    private YSQLExpression onClause;
    private final YSQLJoinType type;

    public YSQLJoin(YSQLExpression tableReference, YSQLExpression onClause, YSQLJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public YSQLExpression getTableReference() {
        return tableReference;
    }

    public YSQLExpression getOnClause() {
        return onClause;
    }

    public YSQLJoinType getType() {
        return type;
    }

    @Override
    public YSQLDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public YSQLConstant getExpectedValue() {
        throw new AssertionError();
    }

    public enum YSQLJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static YSQLJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    @Override
    public void setOnClause(YSQLExpression onClause) {
        this.onClause = onClause;
    }
}
