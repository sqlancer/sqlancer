package sqlancer.sqlite3.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.SQLite3StateToReproduce;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3ToStringVisitor;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import sqlancer.sqlite3.ast.SQLite3Cast;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.Join.JoinType;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm.Ordering;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import sqlancer.sqlite3.ast.SQLite3WindowFunction;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3RowValue;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3PivotedQuerySynthesisOracle implements TestOracle {

    private final Connection database;
    private final SQLite3Schema s;
    private final Randomly r;
    private SQLite3StateToReproduce state;
    private SQLite3RowValue rw;
    private List<SQLite3Column> fetchColumns;
    private final List<String> errors = new ArrayList<>();
    private List<SQLite3Expression> colExpressions;
    private final SQLite3GlobalState globalState;

    public SQLite3PivotedQuerySynthesisOracle(SQLite3GlobalState globalState) throws SQLException {
        this.database = globalState.getConnection();
        this.r = globalState.getRandomly();
        this.globalState = globalState;
        s = SQLite3Schema.fromConnection(globalState);
    }

    @Override
    public void check() throws SQLException {
        Query query = getQueryThatContainsAtLeastOneRow(globalState);
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(query.getQueryString());
        }
        boolean isContainedIn = isContainedIn(query);
        if (!isContainedIn) {
            throw new AssertionError(query);
        }
    }

    public Query getQueryThatContainsAtLeastOneRow(SQLite3GlobalState state) throws SQLException {
        SQLite3Select selectStatement = getQuery(state);
        SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
        visitor.visit(selectStatement);
        String queryString = visitor.get();
        addExpectedErrors(errors);
        return new QueryAdapter(queryString, errors);
    }

    public static void addExpectedErrors(List<String> errors) {
        errors.add("no such index");
        errors.add("no query solution");
        errors.add(
                "[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)");
        errors.add("[SQLITE_ERROR] SQL error or missing database (integer overflow)");
        errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("misuse of aggregate");
        errors.add("GROUP BY term out of range");
    }

    public SQLite3Select getQuery(SQLite3GlobalState globalState) throws SQLException {
        this.state = (SQLite3StateToReproduce) globalState.getState();
        if (s.getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        SQLite3Tables randomFromTables = s.getRandomTableNonEmptyTables();
        List<SQLite3Table> tables = randomFromTables.getTables();

        globalState.getState().queryTargetedTablesString = randomFromTables.tableNamesAsString();
        SQLite3Select selectStatement = new SQLite3Select();
        selectStatement.setSelectType(Randomly.fromOptions(SQLite3Select.SelectType.values()));
        List<SQLite3Column> columns = randomFromTables.getColumns();
        for (SQLite3Table t : tables) {
            if (t.getRowid() != null) {
                columns.add(t.getRowid());
            }
        }
        rw = randomFromTables.getRandomRowValue(database, (SQLite3StateToReproduce) globalState.getState());

        List<Join> joinStatements = new ArrayList<>();
        for (int i = 1; i < tables.size(); i++) {
            SQLite3Expression joinClause = generateWhereClauseThatContainsRowValue(columns, rw);
            SQLite3Table table = Randomly.fromList(tables);
            tables.remove(table);
            JoinType options;
            options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER);
            if (options == JoinType.OUTER && tables.size() > 2) {
                errors.add("ON clause references tables to its right");
            }
            Join j = new SQLite3Expression.Join(table, joinClause, options);
            joinStatements.add(j);
        }
        selectStatement.setJoinClauses(joinStatements);
        selectStatement.setFromTables(SQLite3Common.getTableRefs(tables, s));

        // TODO: also implement a wild-card check (*)
        // filter out row ids from the select because the hinder the reduction process
        // once a bug is found
        List<SQLite3Column> columnsWithoutRowid = columns.stream().filter(c -> !c.getName().matches("rowid"))
                .collect(Collectors.toList());
        fetchColumns = Randomly.nonEmptySubset(columnsWithoutRowid);
        colExpressions = new ArrayList<>();
        List<SQLite3Table> allTables = new ArrayList<>();
        allTables.addAll(tables);
        allTables.addAll(joinStatements.stream().map(join -> join.getTable()).collect(Collectors.toList()));
        boolean allTablesContainOneRow = allTables.stream().allMatch(t -> t.getNrRows() == 1);
        for (SQLite3Column c : fetchColumns) {
            SQLite3Expression colName = new SQLite3ColumnName(c, rw.getValues().get(c));
            if (allTablesContainOneRow && Randomly.getBoolean()) {
                boolean generateDistinct = Randomly.getBoolean();
                if (generateDistinct) {
                    colName = new SQLite3Distinct(colName);
                }
                SQLite3AggregateFunction aggFunc = SQLite3AggregateFunction.getRandom(c.getType());
                colName = new SQLite3Aggregate(Arrays.asList(colName), aggFunc);
                if (Randomly.getBoolean() && !generateDistinct) {
                    colName = generateWindowFunction(columns, colName, true);
                }
                errors.add("second argument to nth_value must be a positive integer");
            }
            if (Randomly.getBoolean()) {
                SQLite3Expression randomExpression;
                do {
                    randomExpression = new SQLite3ExpressionGenerator(globalState).setColumns(columns)
                            .generateExpression();
                } while (randomExpression.getExpectedValue() == null);
                colExpressions.add(randomExpression);
            } else {
                colExpressions.add(colName);
            }
        }
        if (Randomly.getBoolean() && allTablesContainOneRow) {
            SQLite3WindowFunction windowFunction = SQLite3WindowFunction.getRandom(columnsWithoutRowid, globalState);
            SQLite3Expression windowExpr = generateWindowFunction(columnsWithoutRowid, windowFunction, false);
            colExpressions.add(windowExpr);
        }
        selectStatement.setFetchColumns(colExpressions);
        globalState.getState().queryTargetedColumnsString = fetchColumns.stream().map(c -> c.getFullQualifiedName())
                .collect(Collectors.joining(", "));
        SQLite3Expression whereClause = generateWhereClauseThatContainsRowValue(columns, rw);
        selectStatement.setWhereClause(whereClause);
        ((SQLite3StateToReproduce) globalState.getState()).whereClause = selectStatement;
        List<SQLite3Expression> groupByClause = generateGroupByClause(columns, rw, allTablesContainOneRow);
        selectStatement.setGroupByClause(groupByClause);
        SQLite3Expression limitClause = generateLimit((long) (Math.pow(globalState.getOptions().getMaxNumberInserts(),
                joinStatements.size() + randomFromTables.getTables().size())));
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            SQLite3Expression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<SQLite3Expression> orderBy = generateOrderBy(columns);
        selectStatement.setOrderByExpressions(orderBy);
        if (!groupByClause.isEmpty() && Randomly.getBoolean()) {
            SQLite3Expression randomExpression = SQLite3Common.getTrueExpression(columns, globalState);
            if (Randomly.getBoolean()) {
                SQLite3AggregateFunction aggFunc = SQLite3AggregateFunction.getRandom();
                randomExpression = new SQLite3Aggregate(Arrays.asList(randomExpression), aggFunc);
            }
            selectStatement.setHavingClause(randomExpression);
        }
        return selectStatement;
    }

    private SQLite3Expression generateOffset() {
        if (Randomly.getBoolean()) {
            // OFFSET 0
            return SQLite3Constant.createIntConstant(0);
        } else {
            return null;
        }
    }

    public static boolean shouldIgnoreException(SQLException e) {
        return e.getMessage().contentEquals("[SQLITE_ERROR] SQL error or missing database (integer overflow)")
                || e.getMessage().startsWith("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)")
                || e.getMessage().startsWith(
                        "[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)")
                || e.getMessage().contains("second argument to nth_value must be a positive integer");
    }

    private boolean isContainedIn(Query query) throws SQLException {
        Statement createStatement;
        createStatement = database.createStatement();

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        addExpectedValues(sb);
        StringBuilder sb2 = new StringBuilder();
        addExpectedValues(sb2);
        state.values = sb2.toString();
        sb.append(" INTERSECT SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getQueryString());
        sb.append(")");
        String resultingQueryString = sb.toString();
        state.getLocalState().log(resultingQueryString);
        Query finalQuery = new QueryAdapter(resultingQueryString, query.getExpectedErrors());
        try (ResultSet result = createStatement.executeQuery(finalQuery.getQueryString())) {
            boolean isContainedIn = !result.isClosed();
            createStatement.close();
            return isContainedIn;
        } catch (SQLException e) {
            for (String exp : finalQuery.getExpectedErrors()) {
                if (e.getMessage().contains(exp)) {
                    return true;
                }
            }
            throw e;
        }
    }

    private void addExpectedValues(StringBuilder sb) {
        for (int i = 0; i < colExpressions.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            SQLite3Constant expectedValue = colExpressions.get(i).getExpectedValue();
            sb.append(SQLite3Visitor.asString(expectedValue));
        }
    }

    public List<SQLite3Expression> generateOrderBy(List<SQLite3Column> columns) {
        List<SQLite3Expression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            SQLite3Expression expr;
            expr = new SQLite3ExpressionGenerator(globalState).setColumns(columns).generateExpression();
            Ordering order = Randomly.fromOptions(Ordering.ASC, Ordering.DESC);
            orderBys.add(new SQLite3OrderingTerm(expr, order));
            // TODO RANDOM()
        }
        // TODO collate
        errors.add("ORDER BY term out of range");
        return orderBys;
    }

    private SQLite3Expression generateLimit(long l) {
        if (Randomly.getBoolean()) {
            return SQLite3Constant.createIntConstant(r.getLong(l, Long.MAX_VALUE));
        } else {
            return null;
        }
    }

    private List<SQLite3Expression> generateGroupByClause(List<SQLite3Column> columns, SQLite3RowValue rw,
            boolean allTablesContainOneRow) {
        errors.add("GROUP BY term out of range");
        if (allTablesContainOneRow && Randomly.getBoolean()) {
            List<SQLite3Expression> collect = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                collect.add(new SQLite3ExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                        .generateExpression());
            }
            return collect;
        }
        if (Randomly.getBoolean()) {
            // ensure that we GROUP BY all columns
            List<SQLite3Expression> collect = columns.stream().map(c -> new SQLite3ColumnName(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
            if (Randomly.getBoolean()) {
                for (int i = 0; i < Randomly.smallNumber(); i++) {
                    collect.add(new SQLite3ExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                            .generateExpression());
                }
            }
            return collect;
        } else {
            return Collections.emptyList();
        }
    }

    private SQLite3Expression generateWhereClauseThatContainsRowValue(List<SQLite3Column> columns, SQLite3RowValue rw) {

        return generateNewExpression(columns, rw);

    }

    private SQLite3Expression generateNewExpression(List<SQLite3Column> columns, SQLite3RowValue rw) {
        do {
            SQLite3Expression expr = new SQLite3ExpressionGenerator(globalState).setRowValue(rw).setColumns(columns)
                    .generateExpression();
            if (expr.getExpectedValue() != null) {
                if (expr.getExpectedValue().isNull()) {
                    return new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.ISNULL, expr);
                }
                if (SQLite3Cast.isTrue(expr.getExpectedValue()).get()) {
                    return expr;
                } else {
                    return new SQLite3UnaryOperation(UnaryOperator.NOT, expr);
                }
            }
        } while (true);
    }

    //
    private SQLite3Expression generateWindowFunction(List<SQLite3Column> columns, SQLite3Expression colName,
            boolean allowFilter) {
        StringBuilder sb = new StringBuilder();
        if (Randomly.getBoolean() && allowFilter) {
            appendFilter(columns, sb);
        }
        sb.append(" OVER ");
        sb.append("(");
        if (Randomly.getBoolean()) {
            appendPartitionBy(columns, sb);
        }
        if (Randomly.getBoolean()) {
            sb.append(SQLite3Common.getOrderByAsString(columns, globalState));
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("RANGE", "ROWS", "GROUPS"));
            sb.append(" ");
            switch (Randomly.fromOptions(FrameSpec.values())) {
            case BETWEEN:
                sb.append("BETWEEN");
                sb.append(" UNBOUNDED PRECEDING AND CURRENT ROW");
                break;
            case UNBOUNDED_PRECEDING:
                sb.append("UNBOUNDED PRECEDING");
                break;
            case CURRENT_ROW:
                sb.append("CURRENT ROW");
                break;
            default:
                throw new AssertionError();
            }
            // sb.append(" BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
            if (Randomly.getBoolean()) {
                sb.append(" EXCLUDE ");
                // "CURRENT ROW", "GROUP"
                sb.append(Randomly.fromOptions("NO OTHERS", "TIES"));
            }
        }
        sb.append(")");
        SQLite3PostfixText windowFunction = new SQLite3PostfixText(colName, sb.toString(), colName.getExpectedValue());
        errors.add("misuse of aggregate");
        return windowFunction;
    }

    private void appendFilter(List<SQLite3Column> columns, StringBuilder sb) {
        sb.append(" FILTER (WHERE ");
        sb.append(SQLite3Visitor.asString(generateWhereClauseThatContainsRowValue(columns, rw)));
        sb.append(")");
    }

    private void appendPartitionBy(List<SQLite3Column> columns, StringBuilder sb) {
        sb.append(" PARTITION BY ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String orderingTerm;
            do {
                orderingTerm = SQLite3Common.getOrderingTerm(columns, globalState);
            } while (orderingTerm.contains("ASC") || orderingTerm.contains("DESC"));
            // TODO investigate
            sb.append(orderingTerm);
        }
    }

    private enum FrameSpec {
        BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW
    }

}
