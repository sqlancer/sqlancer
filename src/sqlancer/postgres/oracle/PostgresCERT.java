package sqlancer.postgres.oracle;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.CERTOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresTableReference;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresCERT extends CERTOracleBase<PostgresGlobalState> implements TestOracle<PostgresGlobalState> {
    private PostgresExpressionGenerator gen;
    private PostgresSelect select;

    public PostgresCERT(PostgresGlobalState globalState){
        super(globalState);
        // PostgresError?
    }

    @Override
    public void check() throws SQLException {
        queryPlan1Sequences = new ArrayList<>();
        queryPlan2Sequences = new ArrayList<>();

        // Generate Random Query
        PostgresTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new PostgresExpressionGenerator(state).setColumns(tables.getColumns());
        List<PostgresExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
        .map(c -> new PostgresColumnValue(c, null)).collect(Collectors.toList()));
        List<PostgresExpression> tableList = tables.getTables().stream().map(t -> new PostgresTableReference(t))
                .collect(Collectors.toList());
        
        select = new PostgresSelect();
        select.setFetchColumns(fetchColumns);
        select.setFromList(tableList);

        select.setSelectType(Randomly.fromOptions(PostgresSelect.SelectType.values()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0));
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(fetchColumns);
            if (Randomly.getBoolean()) {
                select.setWhereClause(gen.generateExpression(0));
            }
        }

        // First query row count
        String queryString1 = PostgresVisitor.asString(select);
        int rowCount1 = getRow(state, queryString1, queryPlan1Sequences);

        boolean increase = mutate(Mutator.JOIN, Mutator.LIMIT);

        // Second Query row count
        String queryString2 = PostgresVisitor.asString(select);
        int rowCount2 = getRow(state, queryString2, queryPlan2Sequences);

        // Check query plan equivalence
        if (DBMSCommon.editDistance(queryPlan1Sequences, queryPlan2Sequences) > 1){
            return;
        }

        // Check results
        if (increase && rowCount1 > rowCount2 || !increase && rowCount1 < rowCount2) {
            throw new AssertionError("Inconsistent result for query: EXPLAIN " + queryString1 + "; --" + rowCount1
            + "\nEXPLAIN " + queryString2 + "; --" + rowCount2);
        }
    }

    @Override
    protected boolean mutateDistinct() {
        PostgresSelect.SelectType selectType = select.getSelectOption();
        if (selectType != PostgresSelect.SelectType.ALL) {
            select.setSelectType(PostgresSelect.SelectType.ALL);
            return true;
        } else {
            select.setSelectType(PostgresSelect.SelectType.DISTINCT);
            return false;
        }
    }

    @Override
    protected boolean mutateWhere() {
        boolean increase = select.getWhereClause() != null;
        if (increase) {
            select.setWhereClause(null);
        } else {
            select.setWhereClause(gen.generateExpression(0));
        }
        return increase;
    }

    @Override
    protected boolean mutateGroupBy() {
        boolean increase = select.getGroupByExpressions().size() > 0;
        if (increase) {
            select.clearGroupByExpressions();
        } else {
            select.setGroupByExpressions(select.getFetchColumns());
        }
        return increase;
    }

    @Override
    protected boolean mutateHaving() {
        if (select.getGroupByExpressions().size() == 0) {
            select.setGroupByExpressions(select.getFetchColumns());
            select.setHavingClause(gen.generateExpression(0));
            return false;
        } else {
            if (select.getHavingClause() == null) {
                select.setHavingClause(gen.generateExpression(0));
                return false;
            } else {
                select.setHavingClause(null);
                return true;
            }
        }
    }

    @Override
    protected boolean mutateAnd() {
        if (select.getWhereClause() == null) {
            select.setWhereClause(gen.generateExpression(0));
        } else {
            PostgresExpression newWhere = new PostgresBinaryLogicalOperation(select.getWhereClause(),
                    gen.generateExpression(0), PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
            select.setWhereClause(newWhere);
        }
        return false;
    }

    @Override
    protected boolean mutateOr() {
        if (select.getWhereClause() == null) {
            select.setWhereClause(gen.generateExpression(0));
            return false;
        } else {
            PostgresExpression newWhere = new PostgresBinaryLogicalOperation(select.getWhereClause(),
                    gen.generateExpression(0), PostgresBinaryLogicalOperation.BinaryLogicalOperator.OR);
            select.setWhereClause(newWhere);
            return true;
        }
    }

    private int getRow(SQLGlobalState<?, ?> globalState, String selectStr, List<String> queryPlanSequences)
            throws AssertionError, SQLException{
        int row = -1;
        String explainQuery = "EXPLAIN " + selectStr;

        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(explainQuery);
            try{
                globalState.getLogger().getCurrentFileWriter().flush();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        // Get row count
        SQLQueryAdapter q = new SQLQueryAdapter(explainQuery, errors);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    int estRows = rs.getInt(10);
                    if (row == -1) {
                        row = estRows;
                    }
                    String operation = rs.getString(2);
                    queryPlanSequences.add(operation);
                }
            }
        } catch (Exception e) {
            throw new AssertionError(q.getQueryString(), e);
        }
        if(row == -1){
            throw new IgnoreMeException();
        }
        return row;
    }
}