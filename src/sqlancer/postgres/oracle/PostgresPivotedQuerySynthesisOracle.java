package sqlancer.postgres.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.PostgresStateToReproduce;
import sqlancer.TestOracle;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresRowValue;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresToStringVisitor;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresPivotedQuerySynthesisOracle implements TestOracle {

    private PostgresStateToReproduce state;
    private PostgresRowValue rw;
    private final Connection database;
    private List<PostgresColumn> fetchColumns;
    private final PostgresSchema s;
    private final MainOptions options;
    private final StateLogger logger;
    private final PostgresGlobalState globalState;

    public PostgresPivotedQuerySynthesisOracle(PostgresGlobalState globalState) throws SQLException {
        this.globalState = globalState;
        this.database = globalState.getConnection();
        this.s = globalState.getSchema();
        options = globalState.getOptions();
        logger = globalState.getLogger();
    }

    @Override
    public void check() throws SQLException {
        String queryString = getQueryThatContainsAtLeastOneRow(state);
        state.queryString = queryString;
        if (options.logEachSelect()) {
            logger.writeCurrent(state.queryString);
        }

        boolean isContainedIn = isContainedIn(queryString, options, logger);
        if (!isContainedIn) {
            throw new AssertionError(queryString);
        }

    }

    public String getQueryThatContainsAtLeastOneRow(PostgresStateToReproduce state) throws SQLException {
        this.state = state;
        PostgresTables randomFromTables = s.getRandomTableNonEmptyTables();

        state.queryTargetedTablesString = randomFromTables.tableNamesAsString();

        PostgresSelect selectStatement = new PostgresSelect();
        selectStatement.setSelectType(Randomly.fromOptions(PostgresSelect.SelectType.values()));
        List<PostgresColumn> columns = randomFromTables.getColumns();
        rw = randomFromTables.getRandomRowValue(database, state);

        fetchColumns = columns;
        selectStatement.setFromList(randomFromTables.getTables().stream().map(t -> new PostgresFromTable(t, false))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new PostgresColumnValue(c, rw.getValues().get(c))).collect(Collectors.toList()));
        state.queryTargetedColumnsString = fetchColumns.stream().map(c -> c.getFullQualifiedName())
                .collect(Collectors.joining(", "));
        PostgresExpression whereClause = generateWhereClauseThatContainsRowValue(columns, rw);
        selectStatement.setWhereClause(whereClause);
        state.whereClause = selectStatement;
        List<PostgresExpression> groupByClause = generateGroupByClause(columns, rw);
        selectStatement.setGroupByExpressions(groupByClause);
        PostgresExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            PostgresExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<PostgresExpression> orderBy = new PostgresExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);

        StringBuilder sb2 = new StringBuilder();
        sb2.append("SELECT * FROM (SELECT 1 FROM ");
        sb2.append(randomFromTables.tableNamesAsString());
        sb2.append(" WHERE ");
        int i = 0;
        for (PostgresColumn c : fetchColumns) {
            if (i++ != 0) {
                sb2.append(" AND ");
            }
            sb2.append(c.getFullQualifiedName());
            if (rw.getValues().get(c).isNull()) {
                sb2.append(" IS NULL");
            } else {
                sb2.append(" = ");
                sb2.append(rw.getValues().get(c).getTextRepresentation());
            }
        }
        sb2.append(") as result;");
        state.queryThatSelectsRow = sb2.toString();

        PostgresToStringVisitor visitor = new PostgresToStringVisitor();
        visitor.visit(selectStatement);
        return visitor.get();
    }

    private List<PostgresExpression> generateGroupByClause(List<PostgresColumn> columns, PostgresRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> PostgresColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private PostgresConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return PostgresConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private PostgresExpression generateOffset() {
        if (Randomly.getBoolean()) {
            // OFFSET 0
            return PostgresConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    private PostgresExpression generateWhereClauseThatContainsRowValue(List<PostgresColumn> columns,
            PostgresRowValue rw) {
        return PostgresExpressionGenerator.generateTrueCondition(columns, rw, globalState);
    }

    private boolean isContainedIn(String queryString, MainOptions options, StateLogger logger) throws SQLException {
        Statement createStatement;
        createStatement = database.createStatement();

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(queryString);
        sb.append(") as result WHERE ");
        int i = 0;
        for (PostgresColumn c : fetchColumns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result." + c.getTable().getName() + c.getName());
            if (rw.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(rw.getValues().get(c).getTextRepresentation());
            }
        }
        String resultingQueryString = sb.toString();
        state.queryString = resultingQueryString;
        if (options.logEachSelect()) {
            logger.writeCurrent(resultingQueryString);
        }
        try (ResultSet result = createStatement.executeQuery(resultingQueryString)) {
            boolean isContainedIn = result.next();
            createStatement.close();
            return isContainedIn;
        } catch (PSQLException e) {
            if (e.getMessage().contains("out of range") || e.getMessage().contains("cannot cast")
                    || e.getMessage().contains("invalid input syntax for ") || e.getMessage().contains("must be type")
                    || e.getMessage().contains("operator does not exist")
                    || e.getMessage().contains("Could not choose a best candidate function.")
                    || e.getMessage().contains("division by zero")
                    || e.getMessage().contains("zero raised to a negative power is undefined")
                    || e.getMessage().contains("canceling statement due to statement timeout")
                    || e.getMessage().contains("operator is not unique")
                    || e.getMessage().contains("could not determine which collation to use for string comparison")) {
                return true;
            } else {
                throw e;
            }
        }
    }

}
