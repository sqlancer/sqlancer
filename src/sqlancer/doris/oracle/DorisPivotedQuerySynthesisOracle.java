package sqlancer.doris.oracle;

import java.util.ArrayList;
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
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.DorisSchema.DorisRowValue;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisSchema.DorisTables;
import sqlancer.doris.ast.DorisColumnValue;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.ast.DorisUnaryPostfixOperation;
import sqlancer.doris.ast.DorisUnaryPrefixOperation;
import sqlancer.doris.gen.DorisNewExpressionGenerator;
import sqlancer.doris.visitor.DorisExpectedValueVisitor;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<DorisGlobalState, DorisRowValue, DorisExpression, SQLConnection> {

    private List<DorisColumn> fetchColumns;

    public DorisPivotedQuerySynthesisOracle(DorisGlobalState globalState) {
        super(globalState);
        DorisErrors.addExpressionErrors(errors);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    protected Query<SQLConnection> getRectifiedQuery() throws Exception {
        DorisTables randomTables = globalState.getSchema().getRandomTableNonEmptyAndViewTables();
        List<DorisColumn> columns = randomTables.getColumns();
        DorisSelect selectStatement = new DorisSelect();
        boolean isDistinct = Randomly.getBoolean();
        selectStatement.setDistinct(isDistinct);
        pivotRow = randomTables.getRandomRowValue(globalState.getConnection());
        fetchColumns = columns;
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new DorisColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        selectStatement.setFromList(randomTables.getTables().stream()
                .map(t -> new TableReferenceNode<DorisExpression, DorisTable>(t)).collect(Collectors.toList()));
        DorisExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(DorisExprToNode.cast(whereClause));
        List<Node<DorisExpression>> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        Node<DorisExpression> limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            Node<DorisExpression> offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(globalState);
        gen.setColumns(columns);
        if (!isDistinct) {
            List<Node<DorisExpression>> constants = new ArrayList<>();
            constants.add(new DorisConstant.DorisIntConstant(
                    Randomly.smallNumber() % selectStatement.getFetchColumns().size() + 1));
            selectStatement.setOrderByClauses(constants);
        }
        return new SQLQueryAdapter(DorisToStringVisitor.asString(selectStatement), errors);
    }

    private DorisExpression generateRectifiedExpression(List<DorisColumn> columns, DorisRowValue pivotRow) {
        DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(globalState).setColumns(columns);
        gen.setRowValue(pivotRow);
        DorisExpression expr = gen.generateExpressionWithExpectedResult(DorisDataType.BOOLEAN);
        DorisExpression result = null;
        if (expr.getExpectedValue().isNull()) {
            result = new DorisUnaryPostfixOperation(expr, DorisUnaryPostfixOperation.DorisUnaryPostfixOperator.IS_NULL);
        } else if (!expr.getExpectedValue().cast(DorisDataType.BOOLEAN).asBoolean()) {
            result = new DorisUnaryPrefixOperation(expr, DorisUnaryPrefixOperation.DorisUnaryPrefixOperator.NOT);
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
        for (DorisColumn c : fetchColumns) {
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

    private DorisColumn getFetchValueAliasedColumn(DorisColumn c) {
        DorisColumn aliasedColumn = new DorisColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType(), false, false);
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    @Override
    protected String getExpectedValues(DorisExpression expr) {
        return DorisExpectedValueVisitor.asExpectedValues(DorisExprToNode.cast(expr));
    }

    private List<Node<DorisExpression>> generateGroupByClause(List<DorisColumn> columns, DorisRowValue rowValue) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> new DorisColumnValue(c, rowValue.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private Node<DorisExpression> generateLimit() {
        if (Randomly.getBoolean()) {
            return DorisConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private Node<DorisExpression> generateOffset() {
        if (Randomly.getBoolean()) {
            return DorisConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

}
