package sqlancer.oceanbase.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oceanbase.OceanBaseErrors;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseRowValue;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTables;
import sqlancer.oceanbase.OceanBaseVisitor;
import sqlancer.oceanbase.ast.OceanBaseColumnReference;
import sqlancer.oceanbase.ast.OceanBaseConstant;
import sqlancer.oceanbase.ast.OceanBaseExpression;
import sqlancer.oceanbase.ast.OceanBaseOrderByTerm;
import sqlancer.oceanbase.ast.OceanBaseOrderByTerm.OceanBaseOrder;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.ast.OceanBaseTableReference;
import sqlancer.oceanbase.ast.OceanBaseUnaryPostfixOperation;
import sqlancer.oceanbase.ast.OceanBaseUnaryPostfixOperation.UnaryPostfixOperator;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;

public class OceanBasePivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<OceanBaseGlobalState, OceanBaseRowValue, OceanBaseExpression, SQLConnection> {

    private List<OceanBaseExpression> fetchColumns;
    private List<OceanBaseColumn> columns;

    public OceanBasePivotedQuerySynthesisOracle(OceanBaseGlobalState globalState) throws SQLException {
        super(globalState);
        OceanBaseErrors.addExpressionErrors(errors);
        errors.add("in 'order clause'");
        errors.add("value is out of range");
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        OceanBaseTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<OceanBaseTable> tables = randomFromTables.getTables();

        OceanBaseSelect selectStatement = new OceanBaseSelect();
        selectStatement.setSelectType(Randomly.fromOptions(OceanBaseSelect.SelectType.values()));
        columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        selectStatement
                .setFromList(tables.stream().map(t -> new OceanBaseTableReference(t)).collect(Collectors.toList()));

        fetchColumns = columns.stream().map(c -> new OceanBaseColumnReference(c, null)).map(d -> d.setRef(true))
                .collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        OceanBaseExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<OceanBaseExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        OceanBaseExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            OceanBaseExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<OceanBaseExpression> orderBy = generateOrderBy(columns);
        selectStatement.setOrderByExpressions(orderBy);

        return new SQLQueryAdapter(OceanBaseVisitor.asString(selectStatement), errors);
    }

    private List<OceanBaseExpression> generateGroupByClause(List<OceanBaseColumn> columns, OceanBaseRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> OceanBaseColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public List<OceanBaseExpression> generateOrderBy(List<OceanBaseColumn> columns) {
        List<OceanBaseExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new OceanBaseOrderByTerm(OceanBaseColumnReference.create(Randomly.fromList(columns), null),
                    OceanBaseOrder.getRandomOrder()));
        }
        return orderBys;
    }

    private OceanBaseConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return OceanBaseConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private OceanBaseExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return OceanBaseConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private OceanBaseExpression generateRectifiedExpression(List<OceanBaseColumn> columns, OceanBaseRowValue rw) {
        OceanBaseExpression expression = new OceanBaseExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                .generateExpression();
        OceanBaseConstant expectedValue = expression.getExpectedValue();
        OceanBaseExpression result;
        if (expectedValue.isNull()) {
            result = new OceanBaseUnaryPostfixOperation(expression, UnaryPostfixOperator.IS_NULL, false);
        } else if (expectedValue.asBooleanNotNull()) {
            result = expression;
        } else {
            result = new OceanBaseUnaryPrefixOperation(expression, OceanBaseUnaryPrefixOperator.NOT);
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
        for (OceanBaseColumn c : columns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            if (pivotRow.getValues().get(c) instanceof OceanBaseConstant.OceanBaseTextConstant) {
                sb.append("concat(");
            }
            sb.append("result." + c.getTable().getName() + c.getName());
            if (pivotRow.getValues().get(c) instanceof OceanBaseConstant.OceanBaseTextConstant) {
                sb.append(",'')");
            }
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
    protected String getExpectedValues(OceanBaseExpression expr) {
        return OceanBaseVisitor.asExpectedValues(expr);
    }
}
