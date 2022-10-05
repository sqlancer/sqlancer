package sqlancer.yugabyte.ysql.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLRowValue;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTables;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;

public class YSQLPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<YSQLGlobalState, YSQLRowValue, YSQLExpression, SQLConnection> {

    private List<YSQLColumn> fetchColumns;

    public YSQLPivotedQuerySynthesisOracle(YSQLGlobalState globalState) throws SQLException {
        super(globalState);
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonFetchErrors(errors);
    }

    /*
     * Prevent name collisions by aliasing the column.
     */
    private YSQLColumn getFetchValueAliasedColumn(YSQLColumn c) {
        YSQLColumn aliasedColumn = new YSQLColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private List<YSQLExpression> generateGroupByClause(List<YSQLColumn> columns, YSQLRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> YSQLColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private YSQLConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return YSQLConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private YSQLExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return YSQLConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    private YSQLExpression generateRectifiedExpression(List<YSQLColumn> columns, YSQLRowValue rw) {
        YSQLExpression expr = new YSQLExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                .generateExpressionWithExpectedResult(YSQLDataType.BOOLEAN);
        YSQLExpression result;
        if (expr.getExpectedValue().isNull()) {
            result = YSQLPostfixOperation.create(expr, YSQLPostfixOperation.PostfixOperator.IS_NULL);
        } else {
            result = YSQLPostfixOperation.create(expr, expr.getExpectedValue().cast(YSQLDataType.BOOLEAN).asBoolean()
                    ? YSQLPostfixOperation.PostfixOperator.IS_TRUE : YSQLPostfixOperation.PostfixOperator.IS_FALSE);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (YSQLColumn c : fetchColumns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append(c.getTable().getName());
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, errors);
    }

    @Override
    public SQLQueryAdapter getRectifiedQuery() throws SQLException {
        YSQLTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        YSQLSelect selectStatement = new YSQLSelect();
        selectStatement.setSelectType(Randomly.fromOptions(YSQLSelect.SelectType.values()));
        List<YSQLColumn> columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        fetchColumns = columns;
        selectStatement.setFromList(randomFromTables.getTables().stream()
                .map(t -> new YSQLSelect.YSQLFromTable(t, false)).collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new YSQLColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        YSQLExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<YSQLExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        YSQLExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            YSQLExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<YSQLExpression> orderBy = new YSQLExpressionGenerator(globalState).setColumns(columns).generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);
        return new SQLQueryAdapter(YSQLVisitor.asString(selectStatement));
    }

    @Override
    protected String getExpectedValues(YSQLExpression expr) {
        return YSQLVisitor.asExpectedValues(expr);
    }

}
