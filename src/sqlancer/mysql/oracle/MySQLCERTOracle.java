package sqlancer.mysql.oracle;

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
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLCERTOracle extends CERTOracleBase<MySQLGlobalState> implements TestOracle<MySQLGlobalState> {
    private MySQLExpressionGenerator gen;
    private MySQLSelect select;

    public MySQLCERTOracle(MySQLGlobalState globalState) {
        super(globalState);
        MySQLErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        queryPlan1Sequences = new ArrayList<>();
        queryPlan2Sequences = new ArrayList<>();

        // Randomly generate a query
        MySQLTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new MySQLExpressionGenerator(state).setColumns(tables.getColumns());
        List<MySQLExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new MySQLColumnReference(c, null)).collect(Collectors.toList()));
        List<MySQLExpression> tableList = tables.getTables().stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());

        select = new MySQLSelect();
        select.setFetchColumns(fetchColumns);
        select.setFromList(tableList);

        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(fetchColumns);
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateExpression());
            }
        }

        // Set the join. Todo: to make it random
        // List<MySQLExpression> joinExpressions = getJoins(tableList, state);
        // select.setJoinList(joinExpressions);

        // Get the result of the first query
        String queryString1 = MySQLVisitor.asString(select);
        int rowCount1 = getRow(state, queryString1, queryPlan1Sequences);

        boolean increase = mutate(Mutator.JOIN, Mutator.LIMIT);

        // Get the result of the second query
        String queryString2 = MySQLVisitor.asString(select);
        int rowCount2 = getRow(state, queryString2, queryPlan2Sequences);

        // Check structural equivalence
        if (DBMSCommon.editDistance(queryPlan1Sequences, queryPlan2Sequences) > 1) {
            return;
        }

        // Check the results
        if (increase && rowCount1 > rowCount2 || !increase && rowCount1 < rowCount2) {
            throw new AssertionError("Inconsistent result for query: EXPLAIN " + queryString1 + "; --" + rowCount1
                    + "\nEXPLAIN " + queryString2 + "; --" + rowCount2);
        }
    }

    @Override
    protected boolean mutateDistinct() {
        MySQLSelect.SelectType selectType = select.getFromOptions();
        if (selectType != MySQLSelect.SelectType.ALL) {
            select.setSelectType(MySQLSelect.SelectType.ALL);
            return true;
        } else {
            select.setSelectType(MySQLSelect.SelectType.DISTINCT);
            return false;
        }
    }

    @Override
    protected boolean mutateWhere() {
        boolean increase = select.getWhereClause() != null;
        if (increase) {
            select.setWhereClause(null);
        } else {
            select.setWhereClause(gen.generateExpression());
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
            select.setHavingClause(gen.generateExpression());
            return false;
        } else {
            if (select.getHavingClause() == null) {
                select.setHavingClause(gen.generateExpression());
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
            select.setWhereClause(gen.generateExpression());
        } else {
            MySQLExpression newWhere = new MySQLBinaryLogicalOperation(select.getWhereClause(),
                    gen.generateExpression(), MySQLBinaryLogicalOperator.AND);
            select.setWhereClause(newWhere);
        }
        return false;
    }

    @Override
    protected boolean mutateOr() {
        if (select.getWhereClause() == null) {
            select.setWhereClause(gen.generateExpression());
            return false;
        } else {
            MySQLExpression newWhere = new MySQLBinaryLogicalOperation(select.getWhereClause(),
                    gen.generateExpression(), MySQLBinaryLogicalOperator.OR);
            select.setWhereClause(newWhere);
            return true;
        }
    }

    // The limit clause only accpets positive integers, which is not supported yet
    // private boolean mutateLimit() {
    // boolean increase = select.getLimitClause() != null;
    // if (increase) {
    // select.setLimitClause(null);
    // } else {
    // select.setLimitClause(gen.generateConstant());
    // }
    // return increase;
    // }

    private int getRow(SQLGlobalState<?, ?> globalState, String selectStr, List<String> queryPlanSequences)
            throws AssertionError, SQLException {
        int row = -1;
        String explainQuery = "EXPLAIN " + selectStr;

        // Log the query
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(explainQuery);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Get the row count
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
        if (row == -1) {
            throw new IgnoreMeException();
        }
        return row;
    }

}
