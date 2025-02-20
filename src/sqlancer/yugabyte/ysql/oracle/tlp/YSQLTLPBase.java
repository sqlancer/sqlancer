package sqlancer.yugabyte.ysql.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.JoinBase.JoinType;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTables;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLJoin;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;

public class YSQLTLPBase extends TernaryLogicPartitioningOracleBase<YSQLExpression, YSQLGlobalState>
        implements TestOracle<YSQLGlobalState> {

    protected YSQLSchema s;
    protected YSQLTables targetTables;
    protected YSQLExpressionGenerator gen;
    protected YSQLSelect select;

    public YSQLTLPBase(YSQLGlobalState state) {
        super(state);
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        List<YSQLTable> tables = targetTables.getTables();
        List<YSQLJoin> joins = getJoinStatements(state, targetTables.getColumns(), tables);
        generateSelectBase(tables, joins);
    }

    public static List<YSQLJoin> getJoinStatements(YSQLGlobalState globalState, List<YSQLColumn> columns,
            List<YSQLTable> tables) {
        List<YSQLJoin> joinStatements = new ArrayList<>();
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            YSQLExpression joinClause = gen.generateExpression(YSQLDataType.BOOLEAN);
            YSQLTable table = Randomly.fromList(tables);
            tables.remove(table);
            JoinType options = JoinType.getRandom();
            YSQLJoin j = new YSQLJoin(new YSQLSelect.YSQLFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            YSQLTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            YSQLSelect.YSQLSubquery subquery = YSQLExpressionGenerator.createSubquery(globalState,
                    String.format("sub%d", i), subqueryTables);
            YSQLExpression joinClause = gen.generateExpression(YSQLDataType.BOOLEAN);
            JoinType options = JoinType.getRandom();
            YSQLJoin j = new YSQLJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    @SuppressWarnings("unchecked")
    protected void generateSelectBase(List<YSQLTable> tables, List<YSQLJoin> joins) {
        List<YSQLExpression> tableList = tables.stream()
                .map(t -> new YSQLSelect.YSQLFromTable(t, Randomly.getBoolean())).collect(Collectors.toList());
        gen = new YSQLExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new YSQLSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses((List<JoinBase<YSQLExpression>>) (List<?>) joins);
        if (Randomly.getBoolean()) {
            select.setForClause(YSQLSelect.ForClause.getRandom());
        }
    }

    List<YSQLExpression> generateFetchColumns() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return Arrays.asList(new YSQLColumnValue(YSQLColumn.createDummy("*"), null));
        }
        List<YSQLExpression> fetchColumns = new ArrayList<>();
        List<YSQLColumn> targetColumns = Randomly.nonEmptySubset(targetTables.getColumns());
        for (YSQLColumn c : targetColumns) {
            fetchColumns.add(new YSQLColumnValue(c, null));
        }
        return fetchColumns;
    }

    @Override
    protected ExpressionGenerator<YSQLExpression> getGen() {
        return gen;
    }

}
