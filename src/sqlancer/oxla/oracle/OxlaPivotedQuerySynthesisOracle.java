package sqlancer.oxla.oracle;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.OxlaToStringVisitor;
import sqlancer.oxla.ast.*;
import sqlancer.oxla.gen.OxlaExpressionGenerator;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaDataType;
import sqlancer.oxla.schema.OxlaRowValue;
import sqlancer.oxla.schema.OxlaTables;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OxlaPivotedQuerySynthesisOracle extends PivotedQuerySynthesisBase<OxlaGlobalState, OxlaRowValue, OxlaExpression, SQLConnection> {
    private List<OxlaColumn> fetchColumns;

    public OxlaPivotedQuerySynthesisOracle(OxlaGlobalState globalState) {
        super(globalState);
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> pivotRowQuery) throws Exception {
        StringBuilder sb = new StringBuilder()
                .append("SELECT * FROM (")
                .append(pivotRowQuery.getUnterminatedQueryString())
                .append(") AS result WHERE ");

        for (int columnIndex = 0; columnIndex < fetchColumns.size(); ++columnIndex) {
            if (columnIndex != 0) {
                sb.append(" AND ");
            }
            final OxlaColumn column = fetchColumns.get(columnIndex);
            sb.append("result.").append(column.getTable().getName()).append(column.getName());
            if (pivotRow.getValues().get(column) instanceof OxlaConstant.OxlaNullConstant) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(column).toString());
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected Query<SQLConnection> getRectifiedQuery() throws Exception {
        final OxlaTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        fetchColumns = randomFromTables.getColumns();

        // WHAT
        final OxlaSelect select = new OxlaSelect();
        select.type = OxlaSelect.SelectType.getRandom();
        select.setFromList(randomFromTables
                .getTables()
                .stream()
                .map(OxlaTableReference::new)
                .collect(Collectors.toList()));

        // FROM
        select.setFetchColumns(fetchColumns
                .stream()
                .map(column -> new OxlaColumnReference(getAliasedColumn(column), pivotRow.getValues().get(column)))
                .collect(Collectors.toList()));

        // WHERE
        {
            OxlaExpression expr = new OxlaExpressionGenerator(globalState)
                    .setColumns(fetchColumns)
                    .setRowValue(pivotRow)
                    .generateExpression(OxlaDataType.BOOLEAN);
            OxlaExpression result = null;
            if (expr.getExpectedValue() instanceof OxlaConstant.OxlaNullConstant) {
                result = new OxlaUnaryPostfixOperation(expr, OxlaUnaryPostfixOperation.IS_NULL)
            } else {
                result = new OxlaUnaryPostfixOperation(expr, expr.getExpectedValue().cast(OxlaDataType.BOOLEAN) ? OxlaUnaryPostfixOperation.OxlaUnaryPostfixOperator.)
            }
            rectifiedPredicates.add(result);
            select.setWhereClause();
        }

        // GROUP BY
        select.setGroupByClause(List.of());
        if (Randomly.getBoolean()) {
            select.setGroupByClause(fetchColumns
                    .stream()
                    .map(column -> new OxlaColumnReference(column, pivotRow.getValues().get(column)))
                    .collect(Collectors.toList()));
        }

        // ORDER BY
        {
            OxlaExpressionGenerator generator = new OxlaExpressionGenerator(globalState);
            generator.setColumns(fetchColumns);
            List<OxlaExpression> orderBy = new ArrayList<>();
            for (int index = 0; index < Randomly.smallNumber(); ++index) {
                orderBy.add(new OxlaColumnReference(Randomly.fromList(fetchColumns), null));
            }
            select.setOrderByClauses(orderBy);
        }

        // LIMIT
        if (Randomly.getBoolean()) {
            select.setLimitClause(OxlaConstant.createInt32Constant(Integer.MAX_VALUE));
        }

        // OFFSET
        if (Randomly.getBoolean()) {
            select.setOffsetClause(OxlaConstant.createInt32Constant(0));
        }

        return new SQLQueryAdapter(OxlaToStringVisitor.asString(select));
    }

    @Override
    protected String getExpectedValues(OxlaExpression expr) {
        return "";
    }

    private OxlaColumn getAliasedColumn(OxlaColumn column) {
        OxlaColumn aliasedColumn = new OxlaColumn(
                column.getName() + " AS " + column.getTable().getName() + column.getName(),
                column.getType());
        aliasedColumn.setTable(column.getTable());
        return aliasedColumn;
    }
}
