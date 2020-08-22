package sqlancer.mysql.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLRowValue;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.MySQLToStringVisitor;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation.UnaryPostfixOperator;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<MySQLGlobalState, MySQLRowValue, MySQLExpression> {

    private List<MySQLExpression> fetchColumns;
    private List<MySQLColumn> columns;

    public MySQLPivotedQuerySynthesisOracle(MySQLGlobalState globalState) throws SQLException {
        super(globalState);
    }

    @Override
    public Query getQueryThatContainsAtLeastOneRow() throws SQLException {
        MySQLTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<MySQLTable> tables = randomFromTables.getTables();

        MySQLSelect selectStatement = new MySQLSelect();
        selectStatement.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        columns = randomFromTables.getColumns();
        // for (MySQLTable t : tables) {
        // if (t.getRowid() != null) {
        // columns.add(t.getRowid());
        // }
        // }
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        // List<Join> joinStatements = new ArrayList<>();
        // for (int i = 1; i < tables.size(); i++) {
        // SQLite3Expression joinClause = generateWhereClauseThatContainsRowValue(columns, rw);
        // Table table = Randomly.fromList(tables);
        // tables.remove(table);
        // JoinType options;
        // if (tables.size() == 2) {
        // // allow outer with arbitrary column order (see error: ON clause references
        // // tables to its right)
        // options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER);
        // } else {
        // options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS);
        // }
        // Join j = new SQLite3Expression.Join(table, joinClause, options);
        // joinStatements.add(j);
        // }
        // selectStatement.setJoinClauses(joinStatements);
        selectStatement.setFromList(tables.stream().map(t -> new MySQLTableReference(t)).collect(Collectors.toList()));

        fetchColumns = columns.stream().map(c -> new MySQLColumnReference(c, null)).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        MySQLExpression whereClause = generateWhereClauseThatContainsRowValue(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<MySQLExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        MySQLExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            MySQLExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<String> modifiers = Randomly.subset("STRAIGHT_JOIN", "SQL_SMALL_RESULT", "SQL_BIG_RESULT", "SQL_NO_CACHE"); // "SQL_BUFFER_RESULT",
                                                                                                                         // "SQL_CALC_FOUND_ROWS",
                                                                                                                         // "HIGH_PRIORITY"
        // TODO: Incorrect usage/placement of 'SQL_BUFFER_RESULT'
        selectStatement.setModifiers(modifiers);
        List<MySQLExpression> orderBy = new MySQLExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByExpressions(orderBy);

        StringBuilder sb2 = new StringBuilder();
        sb2.append("SELECT * FROM (SELECT 1 FROM ");
        sb2.append(randomFromTables.tableNamesAsString());
        sb2.append(" WHERE ");
        int i = 0;
        for (MySQLColumn c : columns) {
            if (i++ != 0) {
                sb2.append(" AND ");
            }
            sb2.append(c.getFullQualifiedName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb2.append(" IS NULL");
            } else {
                sb2.append(" = ");
                sb2.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        sb2.append(") as result;");

        MySQLToStringVisitor visitor = new MySQLToStringVisitor();
        visitor.visit(selectStatement);
        return new QueryAdapter(visitor.get(), ExpectedErrors.from("BIGINT value is out of range"));
    }

    private List<MySQLExpression> generateGroupByClause(List<MySQLColumn> columns, MySQLRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> MySQLColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private MySQLConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return MySQLConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private MySQLExpression generateOffset() {
        if (Randomly.getBoolean()) {
            // OFFSET 0
            return MySQLConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private MySQLExpression generateWhereClauseThatContainsRowValue(List<MySQLColumn> columns, MySQLRowValue rw) {
        MySQLExpression expression = new MySQLExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                .generateExpression();
        MySQLConstant expectedValue = expression.getExpectedValue();
        if (expectedValue.isNull()) {
            return new MySQLUnaryPostfixOperation(expression, UnaryPostfixOperator.IS_NULL, false);
        } else if (expectedValue.asBooleanNotNull()) {
            return expression;
        } else {
            return new MySQLUnaryPrefixOperation(expression, MySQLUnaryPrefixOperator.NOT);
        }
    }

    @Override
    protected boolean isContainedIn(Query query) throws SQLException {
        Statement createStatement;
        createStatement = globalState.getConnection().createStatement();

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (MySQLColumn c : columns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append(c.getTable().getName() + c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }

        String resultingQueryString = sb.toString();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(resultingQueryString);
        }
        try (ResultSet result = createStatement.executeQuery(resultingQueryString)) {
            boolean isContainedIn = result.next();
            createStatement.close();
            return isContainedIn;
        }
    }

    @Override
    protected String asString(MySQLExpression expr) {
        return MySQLVisitor.asString(expr);
    }
}
