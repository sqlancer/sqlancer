package sqlancer.postgres.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.PostgresStateToReproduce;
import sqlancer.TestOracle;
import sqlancer.postgres.PostgresCompoundDataType;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.ast.PostgresPostfixText;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.SelectType;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresNoRECOracle implements TestOracle {

    private final PostgresSchema s;
    private final Connection con;
    private final PostgresStateToReproduce state;
    private String firstQueryString;
    private String secondQueryString;
    private final StateLogger logger;
    private final MainOptions options;
    private final Set<String> errors = new HashSet<>();
    private final PostgresGlobalState globalState;

    public PostgresNoRECOracle(PostgresGlobalState globalState) {
        this.s = globalState.getSchema();
        this.con = globalState.getConnection();
        this.state = (PostgresStateToReproduce) globalState.getState();
        this.logger = globalState.getLogger();
        this.options = globalState.getOptions();
        this.globalState = globalState;
    }

    @Override
    public void check() throws SQLException {
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
        PostgresTables randomTables = s.getRandomTableNonEmptyTables();
        List<PostgresColumn> columns = randomTables.getColumns();
        PostgresExpression randomWhereCondition = getRandomWhereCondition(columns);
        List<PostgresTable> tables = randomTables.getTables();

        List<PostgresJoin> joinStatements = getJoinStatements(globalState, columns, tables);
        List<PostgresExpression> fromTables = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        int secondCount = getUnoptimizedQueryCount(fromTables, randomWhereCondition, joinStatements);
        int firstCount = getOptimizedQueryCount(fromTables, columns, randomWhereCondition, joinStatements);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            state.queryString = firstCount + " " + secondCount + " " + firstQueryString + ";\n" + secondQueryString
                    + ";";
            throw new AssertionError(firstQueryString + secondQueryString + firstCount + " " + secondCount);
        }
    }

    public static List<PostgresJoin> getJoinStatements(PostgresGlobalState globalState, List<PostgresColumn> columns,
            List<PostgresTable> tables) {
        List<PostgresJoin> joinStatements = new ArrayList<>();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            PostgresExpression joinClause = gen.generateExpression(PostgresDataType.BOOLEAN);
            PostgresTable table = Randomly.fromList(tables);
            tables.remove(table);
            PostgresJoinType options = PostgresJoinType.getRandom();
            PostgresJoin j = new PostgresJoin(table, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    private PostgresExpression getRandomWhereCondition(List<PostgresColumn> columns) {
        return new PostgresExpressionGenerator(globalState).setColumns(columns).setGlobalState(globalState)
                .generateExpression(PostgresDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<PostgresExpression> fromTables, PostgresExpression randomWhereCondition,
            List<PostgresJoin> joinStatements) throws SQLException {
        PostgresSelect select = new PostgresSelect();
        PostgresCastOperation isTrue = new PostgresCastOperation(randomWhereCondition,
                PostgresCompoundDataType.create(PostgresDataType.INT));
        PostgresPostfixText asText = new PostgresPostfixText(isTrue, " as count", null, PostgresDataType.INT);
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(fromTables);
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int secondCount = 0;
        secondQueryString = "SELECT SUM(count) FROM (" + PostgresVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(secondQueryString);
        }
        errors.add("canceling statement due to statement timeout");
        Query q = new QueryAdapter(secondQueryString, errors);
        ResultSet rs;
        try {
            rs = q.executeAndGet(globalState);
        } catch (Exception e) {
            throw new AssertionError(secondQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(List<PostgresExpression> randomTables, List<PostgresColumn> columns,
            PostgresExpression randomWhereCondition, List<PostgresJoin> joinStatements) throws SQLException {
        PostgresSelect select = new PostgresSelect();
        PostgresColumnValue allColumns = new PostgresColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(Arrays.asList(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new PostgresExpressionGenerator(globalState).setColumns(columns)
                    .setGlobalState(globalState).generateOrderBy());
        }
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            firstQueryString = PostgresVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(firstQueryString);
            }
            try (ResultSet rs = stat.executeQuery(firstQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
