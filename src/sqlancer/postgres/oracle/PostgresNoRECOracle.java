package sqlancer.postgres.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class PostgresNoRECOracle implements TestOracle<PostgresGlobalState> {

    private final NoRECOracle<PostgresSelect, PostgresJoin, PostgresExpression, PostgresSchema, PostgresTable, PostgresColumn, PostgresGlobalState> oracle;

    public PostgresNoRECOracle(PostgresGlobalState globalState) {
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                .with(PostgresCommon.getCommonFetchErrors()).withRegex(PostgresCommon.getCommonExpressionRegexErrors())
                .build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

    @Override
    public Reproducer<PostgresGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
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
            PostgresJoin j = new PostgresJoin(new PostgresFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            PostgresTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            PostgresSubquery subquery = PostgresTLPBase.createSubquery(globalState, String.format("sub%d", i),
                    subqueryTables);
            PostgresExpression joinClause = gen.generateExpression(PostgresDataType.BOOLEAN);
            PostgresJoinType options = PostgresJoinType.getRandom();
            PostgresJoin j = new PostgresJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }
}
