package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.TernaryLogicPartitioningOracleBase;
import sqlancer.TestOracle;
import sqlancer.gen.ExpressionGenerator;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.ForClause;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.PostgresNoRECOracle;

public class PostgresTLPBase extends TernaryLogicPartitioningOracleBase<PostgresExpression, PostgresGlobalState>
        implements TestOracle {

    PostgresSchema s;
    PostgresTables targetTables;
    PostgresExpressionGenerator gen;
    PostgresSelect select;

    public PostgresTLPBase(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        // clear left-over query string from previous test
        state.getState().queryString = null;
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new PostgresExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new PostgresSelect();
        select.setFetchColumns(generateFetchColumns());
        List<PostgresTable> tables = targetTables.getTables();
        List<PostgresJoin> joins = PostgresNoRECOracle.getJoinStatements(state, targetTables.getColumns(), tables);
        List<PostgresExpression> tableList = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        // TODO joins
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses(joins);
        if (Randomly.getBoolean()) {
            select.setForClause(ForClause.getRandom());
        }
    }

    List<PostgresExpression> generateFetchColumns() {
        return Arrays.asList(new PostgresColumnValue(targetTables.getColumns().get(0), null));
    }

    @Override
    protected ExpressionGenerator<PostgresExpression> getGen() {
        return gen;
    }

    public static PostgresSubquery createSubquery(PostgresGlobalState globalState, String name) {
        List<PostgresExpression> columns = new ArrayList<>();
        PostgresTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            columns.add(gen.generateExpression(0));
        }
        PostgresSelect select = new PostgresSelect();
        select.setFromList(tables.getTables().stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, PostgresDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(
                        PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(ForClause.getRandom());
        }
        return new PostgresSubquery(select, name);
    }

}
