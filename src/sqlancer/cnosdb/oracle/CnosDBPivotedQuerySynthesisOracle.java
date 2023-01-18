package sqlancer.cnosdb.oracle;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBExpectedError;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBRowValue;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTables;
import sqlancer.cnosdb.CnosDBVisitor;
import sqlancer.cnosdb.ast.*;
import sqlancer.cnosdb.ast.CnosDBPostfixOperation.PostfixOperator;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBFromTable;
import sqlancer.cnosdb.client.CnosDBConnection;
import sqlancer.cnosdb.gen.CnosDBExpressionGenerator;
import sqlancer.cnosdb.query.CnosDBQueryAdapter;
import sqlancer.cnosdb.query.CnosDBSelectQuery;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CnosDBPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<CnosDBGlobalState, CnosDBRowValue, CnosDBExpression, CnosDBConnection> {

    private List<CnosDBColumn> fetchColumns;

    public CnosDBPivotedQuerySynthesisOracle(CnosDBGlobalState globalState) {
        super(globalState);
    }

    @Override
    public CnosDBQueryAdapter getRectifiedQuery() throws SQLException {
        errors.addAll(CnosDBExpectedError.Errors());
        CnosDBTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        CnosDBSelect selectStatement = new CnosDBSelect();
        selectStatement.setSelectType(Randomly.fromOptions(CnosDBSelect.SelectType.values()));
        List<CnosDBColumn> columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        fetchColumns = columns;
        selectStatement.setFromList(randomFromTables.getTables().stream().map(t -> new CnosDBFromTable(t, false))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new CnosDBColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        CnosDBExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<CnosDBExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        CnosDBExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            CnosDBExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<CnosDBExpression> orderBy = new CnosDBExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);
        return new CnosDBSelectQuery(CnosDBVisitor.asString(selectStatement), errors);
    }

    /*
     * Prevent name collisions by aliasing the column.
     */
    private CnosDBColumn getFetchValueAliasedColumn(CnosDBColumn c) {
        CnosDBColumn aliasedColumn = new CnosDBColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private List<CnosDBExpression> generateGroupByClause(List<CnosDBColumn> columns, CnosDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> CnosDBColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private CnosDBConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return CnosDBConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private CnosDBExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return CnosDBConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    private CnosDBExpression generateRectifiedExpression(List<CnosDBColumn> columns, CnosDBRowValue rw) {
        CnosDBExpression expr = new CnosDBExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                .generateExpressionWithExpectedResult(CnosDBDataType.BOOLEAN);
        CnosDBExpression result;
        if (expr.getExpectedValue().isNull()) {
            result = CnosDBPostfixOperation.create(expr, PostfixOperator.IS_NULL);
        } else {
            result = CnosDBPostfixOperation.create(expr,
                    expr.getExpectedValue().cast(CnosDBDataType.BOOLEAN).asBoolean() ? PostfixOperator.IS_TRUE
                            : PostfixOperator.IS_FALSE);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected CnosDBQueryAdapter getContainmentCheckQuery(Query<?> query) throws SQLException {
        errors.addAll(CnosDBExpectedError.Errors());
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (CnosDBColumn c : fetchColumns) {
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
        return new CnosDBSelectQuery(resultingQueryString, errors);
    }

    @Override
    protected String getExpectedValues(CnosDBExpression expr) {
        return CnosDBVisitor.asExpectedValues(expr);
    }

}
