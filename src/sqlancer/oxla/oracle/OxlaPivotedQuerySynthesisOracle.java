package sqlancer.oxla.oracle;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaExpectedValueVisitor;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.*;
import sqlancer.oxla.gen.OxlaExpressionGenerator;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaRowValue;
import sqlancer.oxla.schema.OxlaTables;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OxlaPivotedQuerySynthesisOracle extends PivotedQuerySynthesisBase<OxlaGlobalState, OxlaRowValue, OxlaExpression, SQLConnection> {
    private List<OxlaColumn> fetchColumns;

    public OxlaPivotedQuerySynthesisOracle(OxlaGlobalState globalState, ExpectedErrors errors) {
        super(globalState);
        this.errors.addAll(errors);
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

        // FROM
        final OxlaSelect select = new OxlaSelect();
        select.type = Randomly.fromOptions(OxlaSelect.SelectType.values());
        select.setFromList(randomFromTables
                .getTables()
                .stream()
                .map(OxlaTableReference::new)
                .collect(Collectors.toList()));

        // WHAT
        select.setFetchColumns(fetchColumns
                .stream()
                .map(column -> new OxlaColumnReference(getAliasedColumn(column), pivotRow.getValues().get(column)))
                .collect(Collectors.toList()));

        // JOIN
        // NOTE: Pivoted Query Synthesis (PQS) oracle does not require any JOIN statements to perform its operations.
        //       Of course, it is possible to tests these queries too, but this requires a bit more of a manual setup
        //       see SQLite3PivotedQuerySynthesisOracle for details.
        select.setJoinClauses(List.of());

        // WHERE
        {
            OxlaExpressionGenerator generator = new OxlaExpressionGenerator(globalState);
            generator.setColumns(fetchColumns);
            generator.setRowValue(pivotRow);
            OxlaExpression expr = generator.generatePredicate();
            OxlaExpression result = null;
            if (expr.getExpectedValue() instanceof OxlaConstant.OxlaNullConstant) {
                result = new OxlaUnaryPostfixOperation(expr, OxlaUnaryPostfixOperation.IS_NULL);
            } else {
                assert expr.getExpectedValue() instanceof OxlaConstant.OxlaBooleanConstant;
                final boolean b = ((OxlaConstant.OxlaBooleanConstant) expr.getExpectedValue()).value;
                result = new OxlaUnaryPostfixOperation(expr, b ? OxlaUnaryPostfixOperation.IS_TRUE : OxlaUnaryPostfixOperation.IS_FALSE);
            }
            rectifiedPredicates.add(result);
            select.setWhereClause(result);
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

        return new SQLQueryAdapter(select.asString(), errors);
    }

    @Override
    protected String getExpectedValues(OxlaExpression expr) {
        return OxlaExpectedValueVisitor.asString(expr);
    }

    private OxlaColumn getAliasedColumn(OxlaColumn column) {
        OxlaColumn aliasedColumn = new OxlaColumn(
                column.getName() + " AS " + column.getTable().getName() + column.getName(),
                column.getType());
        aliasedColumn.setTable(column.getTable());
        return aliasedColumn;
    }
}
