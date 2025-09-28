package sqlancer.tidb.oracle;


import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.ast.*;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class TiDBPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<TiDBProvider.TiDBGlobalState, TiDBSchema.TiDBRowValue, TiDBExpression, SQLConnection> {

    private List<TiDBExpression> fetchColumns;
    private List<TiDBSchema.TiDBColumn> columns;

    public TiDBPivotedQuerySynthesisOracle(TiDBProvider.TiDBGlobalState globalState) throws SQLException {
        super(globalState);
        TiDBErrors.addExpressionErrors(errors);
        errors.add("in 'order clause'"); // e.g., Unknown column '2067708013' in 'order clause'
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        TiDBSchema.TiDBTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<TiDBSchema.TiDBTable> tables = randomFromTables.getTables();

        TiDBSelect selectStatement = new TiDBSelect();
        selectStatement.setSelectType(Randomly.fromOptions(TiDBSelect.SelectType.values()));
        columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        selectStatement.setFromList(tables.stream().map(t -> new TiDBTableReference(t)).collect(Collectors.toList()));

        fetchColumns = columns.stream().map(c -> new TiDBColumnReference(c, null)).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        TiDBExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<TiDBExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        TiDBExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            TiDBExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<String> modifiers = Randomly.subset("STRAIGHT_JOIN", "SQL_SMALL_RESULT", "SQL_BIG_RESULT", "SQL_NO_CACHE");
        selectStatement.setModifiers(modifiers);
        List<TiDBExpression> orderBy = new TiDBExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByClauses(orderBy);

        return new SQLQueryAdapter(TiDBVisitor.asString(selectStatement), errors);
    }

    private List<TiDBExpression> generateGroupByClause(List<TiDBColumn> columns, TiDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> TiDBColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private TiDBConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return TiDBConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private TiDBExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return TiDBConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private TiDBExpression generateRectifiedExpression(List<TiDBSchema.TiDBColumn> columns, TiDBSchema.TiDBRowValue rw) {
        TiDBExpression expression = new TiDBExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                .generateExpression();
        TiDBConstant expectedValue = expression.getExpectedValue();
        TiDBExpression result;
        if (expectedValue.isNull()) {
            result = new TiDBUnaryPostfixOperation(expression, TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator.IS_NULL, false);
        } else if (expectedValue.asBooleanNotNull()) {
            result = expression;
        } else {
            result = new TiDBUnaryPrefixOperation(expression, TiDBUnaryPrefixOperation.TiDBUnaryPrefixOperator.NOT);
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
        for (TiDBColumn c : columns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append("ref");
            sb.append(i - 1);
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }

        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, query.getExpectedErrors());
    }

    @Override
    protected String getExpectedValues(TiDBExpression expr) {
        return TiDBVisitor.asExpectedValues(expr);
    }
}
