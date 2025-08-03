package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.beust.jcommander.Strings;

import sqlancer.Randomly;
import sqlancer.common.oracle.DQEBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryError;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractTable;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLDQEOracle extends DQEBase<MySQLGlobalState> implements TestOracle<MySQLGlobalState> {

    private final MySQLSchema schema;
    private static final String APPEND_ORDER_BY = "%s ORDER BY %s";
    private static final String APPEND_LIMIT = "%s LIMIT %d";
    private final List<String> orderColumns = new ArrayList<>();
    private boolean generateLimit;
    private boolean generateOrderBy;
    private boolean operateOnSingleTable;
    private int limit;

    public MySQLDQEOracle(MySQLGlobalState state) {
        super(state);
        schema = state.getSchema();

        MySQLErrors.addExpressionErrors(selectExpectedErrors);

        MySQLErrors.addExpressionErrors(updateExpectedErrors);
        updateExpectedErrors.add("cannot be null");
        updateExpectedErrors.add("Duplicate entry");
        updateExpectedErrors.add("The value specified for generated column");

        MySQLErrors.addExpressionErrors(deleteExpectedErrors);
        deleteExpectedErrors.add("a foreign key constraint fails");
    }

    @Override
    public String generateSelectStatement(MySQLTables tables, String tableName, String whereClauseStr) {
        operateOnSingleTable = tables.getTables().size() == 1;
        List<String> selectColumns = new ArrayList<>();
        for (MySQLTable table : tables.getTables()) {
            selectColumns.add(table.getName() + "." + COLUMN_ROWID);
        }
        if (operateOnSingleTable && Randomly.getBooleanWithSmallProbability()) {
            generateOrderBy = true;
            // generate order by columns
            for (MySQLColumn column : Randomly.nonEmptySubset(tables.getColumns())) {
                orderColumns.add(column.getFullQualifiedName());
            }

            if (Randomly.getBooleanWithRatherLowProbability()) {
                generateLimit = true;
                limit = (int) Randomly.getNotCachedInteger(1, 10);
            }
        }

        String selectStmt = String.format("SELECT %s FROM %s WHERE %s", Strings.join(",", selectColumns).toLowerCase(),
                tableName, whereClauseStr);
        if (generateOrderBy) {
            selectStmt = String.format(APPEND_ORDER_BY, selectStmt, String.join(",", orderColumns));
            if (generateLimit) {
                selectStmt = String.format(APPEND_LIMIT, selectStmt, limit);
            }
        }
        return selectStmt;
    }

    @Override
    public String generateUpdateStatement(MySQLTables tables, String tableName, String whereClauseStr) {
        List<String> updateColumns = new ArrayList<>();
        for (MySQLTable table : tables.getTables()) {
            updateColumns.add(String.format("%s = 1", table.getName() + "." + COLUMN_UPDATED));
        }
        String updateStmt = String.format("UPDATE %s SET %s WHERE %s", tableName, Strings.join(",", updateColumns),
                whereClauseStr);
        if (generateOrderBy) {
            updateStmt = String.format(APPEND_ORDER_BY, updateStmt, String.join(",", orderColumns));
            if (generateLimit) {
                updateStmt = String.format(APPEND_LIMIT, updateStmt, limit);
            }
        }
        return updateStmt;
    }

    @Override
    public String generateDeleteStatement(MySQLTables tables, String tableName, String whereClauseStr) {
        String deleteStmt;
        if (operateOnSingleTable) {
            deleteStmt = String.format("DELETE FROM %s WHERE %s", tableName, whereClauseStr);
            if (generateOrderBy) {
                deleteStmt = String.format(APPEND_ORDER_BY, deleteStmt, String.join(",", orderColumns));
                if (generateLimit) {
                    deleteStmt = String.format(APPEND_LIMIT, deleteStmt, limit);
                }
            }
        } else {
            deleteStmt = String.format("DELETE %s FROM %s WHERE %s", tableName, tableName, whereClauseStr);
        }
        return deleteStmt;
    }

    @Override
    public void check() throws SQLException {

        MySQLTables tables = schema.getRandomTableNonEmptyTables();
        String tableName = tables.getTables().stream().map(AbstractTable::getName).collect(Collectors.joining(","));

        // DQE does not support aggregate functions, windows functions
        // This method does not generate them, may need some configurations if they can be generated
        MySQLExpressionGenerator expressionGenerator = new MySQLExpressionGenerator(state)
                .setColumns(tables.getColumns());
        MySQLExpression whereClause = expressionGenerator.generateExpression();

        // MySQLVisitor is not deterministic, we should keep it only once.
        // Especially, in MySQLUnaryPostfixOperation and MySQLUnaryPrefixOperation
        String whereClauseStr = MySQLVisitor.asString(whereClause);

        // Generate a SELECT statement
        String selectStmt = generateSelectStatement(tables, tableName, whereClauseStr);

        // Generate an UPDATE statement
        String updateStmt = generateUpdateStatement(tables, tableName, whereClauseStr);

        // Generate a DELETE statement
        String deleteStmt = generateDeleteStatement(tables, tableName, whereClauseStr);

        for (MySQLTable table : tables.getTables()) {
            addAuxiliaryColumns(table);
        }

        state.getState().getLocalState().log(selectStmt);
        SQLQueryResult selectExecutionResult = executeSelect(selectStmt, tables);
        state.getState().getLocalState().log(selectExecutionResult.getAccessedRows().values().toString());
        state.getState().getLocalState().log(selectExecutionResult.getQueryErrors().toString());

        state.getState().getLocalState().log(updateStmt);
        SQLQueryResult updateExecutionResult = executeUpdate(updateStmt, tables);
        state.getState().getLocalState().log(updateExecutionResult.getAccessedRows().values().toString());
        state.getState().getLocalState().log(updateExecutionResult.getQueryErrors().toString());

        state.getState().getLocalState().log(deleteStmt);
        SQLQueryResult deleteExecutionResult = executeDelete(deleteStmt, tables);
        state.getState().getLocalState().log(deleteExecutionResult.getAccessedRows().values().toString());
        state.getState().getLocalState().log(deleteExecutionResult.getQueryErrors().toString());

        String compareSelectAndUpdate = compareSelectAndUpdate(selectExecutionResult, updateExecutionResult);
        String compareSelectAndDelete = compareSelectAndDelete(selectExecutionResult, deleteExecutionResult);
        String compareUpdateAndDelete = compareUpdateAndDelete(updateExecutionResult, deleteExecutionResult);

        String errorMessage = compareSelectAndUpdate == null ? "" : compareSelectAndUpdate + "\n";
        errorMessage += compareSelectAndDelete == null ? "" : compareSelectAndDelete + "\n";
        errorMessage += compareUpdateAndDelete == null ? "" : compareUpdateAndDelete + "\n";

        if (!errorMessage.equals("")) {
            throw new AssertionError(errorMessage);
        } else {
            state.getState().getLocalState().log("PASS");
        }

        for (MySQLTable table : tables.getTables()) {
            dropAuxiliaryColumns(table);
        }
    }

    public String compareSelectAndUpdate(SQLQueryResult selectResult, SQLQueryResult updateResult) {
        if (updateResult.hasEmptyErrors()) {
            if (selectResult.hasErrors()) {
                return "SELECT has errors, but UPDATE does not.";
            }
            if (!selectResult.hasSameAccessedRows(updateResult)) {
                return "SELECT accessed different rows from UPDATE.";
            }
            return null;
        } else { // update has errors
            if (hasUpdateSpecificErrors(updateResult)) {
                if (updateResult.hasAccessedRows()) {
                    return "UPDATE accessed non-empty rows when specific errors happen.";
                } else {
                    // we do not compare update with select when update has specific errors
                    return null;
                }
            }

            // update errors should all appear in the select errors
            List<SQLQueryError> queryErrors = new ArrayList<>(selectResult.getQueryErrors());
            for (int i = 0; i < updateResult.getQueryErrors().size(); i++) {
                SQLQueryError updateError = updateResult.getQueryErrors().get(i);
                boolean found = false;
                for (int j = 0; j < queryErrors.size(); j++) {
                    SQLQueryError selectError = queryErrors.get(j);
                    if (selectError.hasSameCodeAndMessage(updateError)) {
                        queryErrors.remove(selectError);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return "SELECT has different errors from UPDATE.";
                }
            }

            if (hasStopErrors(updateResult)) {
                if (updateResult.hasAccessedRows()) {
                    return "UPDATE accessed non-empty rows when stop errors happen.";
                }
            } else {
                if (!selectResult.hasSameAccessedRows(updateResult)) {
                    return "SELECT accessed different rows from UPDATE when errors happen.";
                }

            }

            return null;
        }
    }

    public String compareSelectAndDelete(SQLQueryResult selectResult, SQLQueryResult deleteResult) {
        if (deleteResult.hasEmptyErrors()) {
            if (selectResult.hasErrors()) {
                return "SELECT has errors, but DELETE does not.";
            }
            if (!selectResult.hasSameAccessedRows(deleteResult)) {
                return "SELECT accessed different rows from DELETE.";
            }
            return null;
        } else { // delete has errors
            if (hasDeleteSpecificErrors(deleteResult)) {
                if (deleteResult.hasAccessedRows()) {
                    return "DELETE accessed non-empty rows when specific errors happen.";
                } else {
                    // we do not compare delete with select when delete has specific errors
                    return null;
                }
            }

            // delete errors should all appear in the select errors
            List<SQLQueryError> queryErrors = new ArrayList<>(selectResult.getQueryErrors());
            for (int i = 0; i < deleteResult.getQueryErrors().size(); i++) {
                SQLQueryError deleteError = deleteResult.getQueryErrors().get(i);
                boolean found = false;
                for (int j = 0; j < queryErrors.size(); j++) {
                    SQLQueryError selectError = queryErrors.get(j);
                    if (selectError.hasSameCodeAndMessage(deleteError)) {
                        queryErrors.remove(deleteError);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return "SELECT has different errors from DELETE.";
                }
            }

            if (hasStopErrors(deleteResult)) {
                if (deleteResult.hasAccessedRows()) {
                    return "DELETE accessed non-empty rows when stop errors happen.";
                }
            } else {
                if (!selectResult.hasSameAccessedRows(deleteResult)) {
                    return "SELECT accessed different rows from DELETE when errors happen.";
                }
            }

            return null;
        }
    }

    public String compareUpdateAndDelete(SQLQueryResult updateResult, SQLQueryResult deleteResult) {
        if (updateResult.hasEmptyErrors() && deleteResult.hasEmptyErrors()) {
            if (updateResult.hasSameAccessedRows(deleteResult)) {
                return null;
            } else {
                return "UPDATE accessed different rows from DELETE.";
            }
        } else { // update or delete has errors
            boolean hasSpecificErrors = false;

            if (hasUpdateSpecificErrors(updateResult)) {
                hasSpecificErrors = true;
                if (updateResult.hasAccessedRows()) {
                    return "UPDATE accessed non-empty rows when specific errors happen.";
                }
            }

            if (hasDeleteSpecificErrors(deleteResult)) {
                hasSpecificErrors = true;
                if (deleteResult.hasAccessedRows()) {
                    return "DELETE accessed non-empty rows when specific errors happen.";
                }
            }

            // when one of these statements has specific errors, do not compare them
            if (hasSpecificErrors) {
                return null;
            }

            if (!updateResult.hasSameErrors(deleteResult)) {
                return "UPDATE has different errors from DELETE.";
            } else {
                if (!hasStopErrors(updateResult)) {
                    if (!updateResult.hasSameAccessedRows(deleteResult)) {
                        return "UPDATE accessed different rows from DELETE.";
                    }
                } else {
                    if (updateResult.hasAccessedRows() || deleteResult.hasAccessedRows()) {
                        return "UPDATE or DELETE accessed non-empty rows when stop errors happen.";
                    }
                }
            }

            return null;
        }
    }

    /*
     * when update violates column constraints, such as not null, unique, primary key and generated column, we cannot
     * compare it with other queries.
     */
    private boolean hasUpdateSpecificErrors(SQLQueryResult updateResult) {
        return updateResult.getQueryErrors().stream().anyMatch(
                error -> new MySQLErrorCodeStrategy().getUpdateSpecificErrorCodes().contains(error.getCode()));
    }

    /*
     * when delete violates column constraints, such as foreign key, we cannot compare it with other queries.
     */
    private boolean hasDeleteSpecificErrors(SQLQueryResult deleteResult) {
        return deleteResult.getQueryErrors().stream().anyMatch(
                error -> new MySQLErrorCodeStrategy().getDeleteSpecificErrorCodes().contains(error.getCode()));

    }

    private boolean hasStopErrors(SQLQueryResult queryResult) {
        return queryResult.getQueryErrors().stream()
                .anyMatch(error -> error.getLevel() == SQLQueryError.ErrorLevel.ERROR);
    }

    private SQLQueryResult executeSelect(String selectStmt, MySQLTables tables) throws SQLException {
        Map<AbstractRelationalTable<?, ?, ?>, Set<String>> accessedRows = new HashMap<>();
        List<SQLQueryError> queryErrors;
        SQLancerResultSet resultSet = null;
        try {
            resultSet = new SQLQueryAdapter(selectStmt, selectExpectedErrors).executeAndGet(state, false);
        } catch (SQLException ignored) {
            // we ignore this error, and use get errors to catch it
        } finally {
            queryErrors = getErrors();

            if (resultSet != null) {
                for (MySQLTable table : tables.getTables()) {
                    HashSet<String> rows = new HashSet<>();
                    accessedRows.put(table, rows);
                }
                while (resultSet.next()) {
                    for (MySQLTable table : tables.getTables()) {
                        accessedRows.get(table).add(resultSet.getString(table.getName() + "." + COLUMN_ROWID));
                    }
                }
                resultSet.close();
            }
        }

        return new SQLQueryResult(accessedRows, queryErrors);
    }

    private SQLQueryResult executeUpdate(String updateStmt, MySQLTables tables) throws SQLException {
        Map<AbstractRelationalTable<?, ?, ?>, Set<String>> accessedRows = new HashMap<>();
        List<SQLQueryError> queryErrors;
        try {
            new SQLQueryAdapter("BEGIN").execute(state, false);
            new SQLQueryAdapter(updateStmt, updateExpectedErrors).execute(state, false);
        } catch (SQLException ignored) {
            // we ignore this error, and we use get errors to catch it
        } finally {
            queryErrors = getErrors();

            for (MySQLTable table : tables.getTables()) {
                String tableName = table.getName();
                String rowId = tableName + "." + COLUMN_ROWID;
                String updated = tableName + "." + COLUMN_UPDATED;
                String selectRowIdWithUpdated = String.format("SELECT %s FROM %s WHERE %s = 1", rowId, tableName,
                        updated);
                SQLancerResultSet resultSet = new SQLQueryAdapter(selectRowIdWithUpdated).executeAndGet(state, false);
                HashSet<String> rows = new HashSet<>();
                if (resultSet != null) {
                    while (resultSet.next()) {
                        rows.add(resultSet.getString(rowId));
                    }
                    resultSet.close();
                }
                accessedRows.put(table, rows);
            }

            new SQLQueryAdapter("ROLLBACK").execute(state, false);
        }

        return new SQLQueryResult(accessedRows, queryErrors);
    }

    private SQLQueryResult executeDelete(String deleteStmt, MySQLTables tables) throws SQLException {
        Map<AbstractRelationalTable<?, ?, ?>, Set<String>> accessedRows = new HashMap<>();
        List<SQLQueryError> queryErrors;
        try {
            for (MySQLTable table : tables.getTables()) {
                String tableName = table.getName();
                String rowId = tableName + "." + COLUMN_ROWID;
                String selectRowId = String.format("SELECT %s FROM %s", rowId, tableName);
                SQLancerResultSet resultSet = new SQLQueryAdapter(selectRowId).executeAndGet(state, false);
                HashSet<String> rows = new HashSet<>();
                if (resultSet != null) {
                    while (resultSet.next()) {
                        rows.add(resultSet.getString(rowId));
                    }
                    resultSet.close();
                }
                accessedRows.put(table, rows);
            }

            new SQLQueryAdapter("BEGIN").execute(state, false);
            new SQLQueryAdapter(deleteStmt, deleteExpectedErrors).execute(state, false);
        } catch (SQLException ignored) {
            // we ignore this error, and use get errors to catch it
        } finally {
            queryErrors = getErrors();

            for (MySQLTable table : tables.getTables()) {
                String tableName = table.getName();
                String rowId = tableName + "." + COLUMN_ROWID;
                String selectRowId = String.format("SELECT %s FROM %s", rowId, tableName);
                SQLancerResultSet resultSet = new SQLQueryAdapter(selectRowId).executeAndGet(state, false);
                HashSet<String> rows = new HashSet<>();
                if (resultSet != null) {
                    while (resultSet.next()) {
                        rows.add(resultSet.getString(rowId));
                    }
                    resultSet.close();
                }

                accessedRows.get(table).removeAll(rows);
            }

            new SQLQueryAdapter("ROLLBACK").execute(state, false);
        }

        return new SQLQueryResult(accessedRows, queryErrors);
    }

    private List<SQLQueryError> getErrors() throws SQLException {
        SQLancerResultSet resultSet = new SQLQueryAdapter("SHOW WARNINGS").executeAndGet(state, false);
        List<SQLQueryError> queryErrors = new ArrayList<>();
        if (resultSet != null) {
            while (resultSet.next()) {
                SQLQueryError queryError = new SQLQueryError();
                queryError.setLevel(resultSet.getErrorLevel("Level"));
                queryError.setCode(resultSet.getInt("Code"));
                queryError.setMessage(resultSet.getString("Message"));
                queryErrors.add(queryError);
            }
            resultSet.close();
        }

        return queryErrors;
    }

    @Override
    public void addAuxiliaryColumns(AbstractRelationalTable<?, ?, ?> table) throws SQLException {
        String tableName = table.getName();

        String addColumnRowID = String.format("ALTER TABLE %s ADD %s TEXT", tableName, COLUMN_ROWID);
        new SQLQueryAdapter(addColumnRowID).execute(state, false);
        state.getState().getLocalState().log(addColumnRowID);

        String addColumnUpdated = String.format("ALTER TABLE %s ADD %s INT DEFAULT 0", tableName, COLUMN_UPDATED);
        new SQLQueryAdapter(addColumnUpdated).execute(state, false);
        state.getState().getLocalState().log(addColumnUpdated);

        String updateRowsWithUniqueID = String.format("UPDATE %s SET %s = UUID()", tableName, COLUMN_ROWID);
        new SQLQueryAdapter(updateRowsWithUniqueID).execute(state, false);
        state.getState().getLocalState().log(updateRowsWithUniqueID);
    }

    public static class MySQLErrorCodeStrategy implements ErrorCodeStrategy {
        @Override
        public Set<Integer> getUpdateSpecificErrorCodes() {
            // 1048, Column 'c0' cannot be null
            // 1062, Duplicate entry '2' for key 't1.i0
            // 3105, The value specified for generated column 'c1' in table 't1' is not allowed
            return Set.of(1048, 1062, 3105);
        }

        @Override
        public Set<Integer> getDeleteSpecificErrorCodes() {
            // 1451, Cannot delete or update a parent row: a foreign key constraint fails
            return Set.of(1451);
        }
    }
}
