package sqlancer.yugabyte.ysql.ast;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public class YSQLJoin extends JoinBase<YSQLExpression> implements YSQLExpression, Join<YSQLExpression, YSQLTable, YSQLColumn> {

    public YSQLJoin(YSQLExpression tableReference, YSQLExpression onClause, JoinBase.JoinType type) {
        super(tableReference, onClause, type);
    }

    public YSQLExpression getTableReference() {
        return tableReference;
    }

    public YSQLExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
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

    @Override
    public void setOnClause(YSQLExpression onClause) {
        this.onClause = onClause;
    }
}
