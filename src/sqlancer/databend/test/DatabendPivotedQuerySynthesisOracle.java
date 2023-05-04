package sqlancer.databend.test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendExpectedValueVisitor;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendSchema.DatabendRowValue;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendSchema.DatabendTables;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendColumnValue;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation;
import sqlancer.databend.ast.DatabendUnaryPrefixOperation;
import sqlancer.databend.gen.DatabendNewExpressionGenerator;

public class DatabendPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<DatabendGlobalState, DatabendRowValue, DatabendExpression, SQLConnection> {

    private List<DatabendColumn> fetchColumns;

    public DatabendPivotedQuerySynthesisOracle(DatabendGlobalState globalState) {
        super(globalState);
        DatabendErrors.addExpressionErrors(errors);
        DatabendErrors.addInsertErrors(errors);
    }

    @Override
    protected Query<SQLConnection> getRectifiedQuery() throws Exception {
        DatabendTables randomTables = globalState.getSchema().getRandomTableNonEmptyAndViewTables();
        List<DatabendColumn> columns = randomTables.getColumns();
        DatabendSelect selectStatement = new DatabendSelect();
        boolean isDistinct = Randomly.getBoolean();
        selectStatement.setDistinct(isDistinct);
        pivotRow = randomTables.getRandomRowValue(globalState.getConnection());
        fetchColumns = columns;
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new DatabendColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        selectStatement.setFromList(randomTables.getTables().stream()
                .map(t -> new TableReferenceNode<DatabendExpression, DatabendTable>(t)).collect(Collectors.toList()));
        DatabendExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(DatabendExprToNode.cast(whereClause));
        List<Node<DatabendExpression>> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        Node<DatabendExpression> limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            Node<DatabendExpression> offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(globalState).setColumns(columns);
        if (!isDistinct) {
            List<Node<DatabendExpression>> orderBys = gen.generateOrderBy();
            selectStatement.setOrderByExpressions(orderBys);
        }
        return new SQLQueryAdapter(DatabendToStringVisitor.asString(selectStatement), errors);
    }

    private DatabendExpression generateRectifiedExpression(List<DatabendColumn> columns, DatabendRowValue pivotRow) {
        DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(globalState).setColumns(columns);
        gen.setRowValue(pivotRow);
        DatabendExpression expr = gen.generateExpressionWithExpectedResult(DatabendDataType.BOOLEAN);
        DatabendExpression result = null;
        if (expr.getExpectedValue().isNull()) {
            result = new DatabendUnaryPostfixOperation(expr,
                    DatabendUnaryPostfixOperation.DatabendUnaryPostfixOperator.IS_NULL);
        } else if (!expr.getExpectedValue().cast(DatabendDataType.BOOLEAN).asBoolean()) {
            result = new DatabendUnaryPrefixOperation(expr,
                    DatabendUnaryPrefixOperation.DatabendUnaryPrefixOperator.NOT);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> pivotRowQuery) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM (");
        sb.append(pivotRowQuery.getUnterminatedQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (DatabendColumn c : fetchColumns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append(c.getTable().getName());
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL ");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).toString());
            }
        }
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, errors);
    }

    private DatabendColumn getFetchValueAliasedColumn(DatabendColumn c) {
        DatabendColumn aliasedColumn = new DatabendColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType(), false, false);
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    @Override
    protected String getExpectedValues(DatabendExpression expr) {
        return DatabendExpectedValueVisitor.asExpectedValues(DatabendExprToNode.cast(expr));
    }

    private List<Node<DatabendExpression>> generateGroupByClause(List<DatabendColumn> columns,
            DatabendRowValue rowValue) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> new DatabendColumnValue(c, rowValue.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private Node<DatabendExpression> generateLimit() {
        if (Randomly.getBoolean()) {
            return DatabendConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private Node<DatabendExpression> generateOffset() {
        if (Randomly.getBoolean()) {
            return DatabendConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

}
