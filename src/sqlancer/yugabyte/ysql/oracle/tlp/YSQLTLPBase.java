package sqlancer.yugabyte.ysql.oracle.tlp;

import static sqlancer.yugabyte.ysql.YSQLUtils.getJoinStatements;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
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
