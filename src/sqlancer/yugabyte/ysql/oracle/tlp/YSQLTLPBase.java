package sqlancer.yugabyte.ysql.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
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
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLJoin;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;
import sqlancer.yugabyte.ysql.oracle.YSQLNoRECOracle;

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

    public static YSQLSelect.YSQLSubquery createSubquery(YSQLGlobalState globalState, String name, YSQLTables tables) {
        List<YSQLExpression> columns = new ArrayList<>();
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            columns.add(gen.generateExpression(0));
        }
        YSQLSelect select = new YSQLSelect();
        select.setFromList(tables.getTables().stream().map(t -> new YSQLSelect.YSQLFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, YSQLDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(YSQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(YSQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(YSQLSelect.ForClause.getRandom());
        }
        return new YSQLSelect.YSQLSubquery(select, name);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        List<YSQLTable> tables = targetTables.getTables();
        List<YSQLJoin> joins = getJoinStatements(state, targetTables.getColumns(), tables);
        generateSelectBase(tables, joins);
    }

    protected List<YSQLJoin> getJoinStatements(YSQLGlobalState globalState, List<YSQLColumn> columns,
            List<YSQLTable> tables) {
        return YSQLNoRECOracle.getJoinStatements(state, columns, tables);
        // TODO joins
    }

    protected void generateSelectBase(List<YSQLTable> tables, List<YSQLJoin> joins) {
        List<YSQLExpression> tableList = tables.stream()
                .map(t -> new YSQLSelect.YSQLFromTable(t, Randomly.getBoolean())).collect(Collectors.toList());
        gen = new YSQLExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new YSQLSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses(joins);
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
