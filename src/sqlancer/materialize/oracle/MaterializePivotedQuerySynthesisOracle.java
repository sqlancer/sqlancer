package sqlancer.materialize.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeRowValue;
import sqlancer.materialize.MaterializeSchema.MaterializeTables;
import sqlancer.materialize.MaterializeVisitor;
import sqlancer.materialize.ast.MaterializeColumnValue;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializePostfixOperation;
import sqlancer.materialize.ast.MaterializePostfixOperation.PostfixOperator;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeFromTable;
import sqlancer.materialize.gen.MaterializeCommon;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;

public class MaterializePivotedQuerySynthesisOracle extends
        PivotedQuerySynthesisBase<MaterializeGlobalState, MaterializeRowValue, MaterializeExpression, SQLConnection> {

    private List<MaterializeColumn> fetchColumns;

    public MaterializePivotedQuerySynthesisOracle(MaterializeGlobalState globalState) throws SQLException {
        super(globalState);
        MaterializeCommon.addCommonExpressionErrors(errors);
        MaterializeCommon.addCommonFetchErrors(errors);
    }

    @Override
    public SQLQueryAdapter getRectifiedQuery() throws SQLException {
        MaterializeTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        MaterializeSelect selectStatement = new MaterializeSelect();
        selectStatement.setSelectType(Randomly.fromOptions(MaterializeSelect.SelectType.values()));
        List<MaterializeColumn> columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        fetchColumns = columns;
        selectStatement.setFromList(randomFromTables.getTables().stream().map(t -> new MaterializeFromTable(t, false))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new MaterializeColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        MaterializeExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<MaterializeExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        MaterializeExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            MaterializeExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<MaterializeExpression> orderBy = new MaterializeExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);
        return new SQLQueryAdapter(MaterializeVisitor.asString(selectStatement));
    }

    /*
     * Prevent name collisions by aliasing the column.
     */
    private MaterializeColumn getFetchValueAliasedColumn(MaterializeColumn c) {
        MaterializeColumn aliasedColumn = new MaterializeColumn(
                c.getName() + " AS " + c.getTable().getName() + c.getName(), c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private List<MaterializeExpression> generateGroupByClause(List<MaterializeColumn> columns, MaterializeRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> MaterializeColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private MaterializeConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return MaterializeConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private MaterializeExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return MaterializeConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    private MaterializeExpression generateRectifiedExpression(List<MaterializeColumn> columns, MaterializeRowValue rw) {
        MaterializeExpression expr = new MaterializeExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                .generateExpressionWithExpectedResult(MaterializeDataType.BOOLEAN);
        MaterializeExpression result;
        if (expr.getExpectedValue().isNull()) {
            result = MaterializePostfixOperation.create(expr, PostfixOperator.IS_NULL);
        } else {
            result = MaterializePostfixOperation.create(expr,
                    expr.getExpectedValue().cast(MaterializeDataType.BOOLEAN).asBoolean() ? PostfixOperator.IS_TRUE
                            : PostfixOperator.IS_FALSE);
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
        for (MaterializeColumn c : fetchColumns) {
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
    protected String getExpectedValues(MaterializeExpression expr) {
        return MaterializeVisitor.asExpectedValues(expr);
    }

}
