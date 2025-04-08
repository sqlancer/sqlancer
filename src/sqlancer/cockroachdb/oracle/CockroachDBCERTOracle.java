package sqlancer.cockroachdb.oracle;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.cockroachdb.CockroachDBBugs;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBJoin.JoinType;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.CERTOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public class CockroachDBCERTOracle extends CERTOracleBase<CockroachDBGlobalState>
        implements TestOracle<CockroachDBGlobalState> {
    private CockroachDBExpressionGenerator gen;
    private CockroachDBSelect select;

    public CockroachDBCERTOracle(CockroachDBGlobalState globalState) {
        super(globalState);
        CockroachDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        queryPlan1Sequences = new ArrayList<>();
        queryPlan2Sequences = new ArrayList<>();

        // Randomly generate a query
        CockroachDBTables tables = state.getSchema().getRandomTableNonEmptyTables(2);
        List<CockroachDBExpression> tableList = CockroachDBCommon.getTableReferences(
                tables.getTables().stream().map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList()));
        gen = new CockroachDBExpressionGenerator(state).setColumns(tables.getColumns());
        List<CockroachDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new CockroachDBColumnReference(c)).collect(Collectors.toList()));
        select = new CockroachDBSelect();
        select.setFetchColumns(fetchColumns);
        select.setFromList(tableList);
        select.setDistinct(Randomly.getBoolean());
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(fetchColumns);
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
            }
        }

        // Set the join.
        List<CockroachDBExpression> joinExpressions = getJoins(tableList, state);
        select.setJoinList(joinExpressions);

        // Get the result of the first query
        String queryString1 = CockroachDBVisitor.asString(select);
        int rowCount1 = getRow(state, queryString1, queryPlan1Sequences);

        List<Mutator> excludes = new ArrayList<>();
        // Disable limit due to its false positive
        excludes.add(Mutator.LIMIT);
        if (CockroachDBBugs.bug131640) {
            excludes.add(Mutator.OR);
        }
        if (CockroachDBBugs.bug131647) {
            excludes.add(Mutator.JOIN);
        }
        // Mutate the query
        boolean increase = mutate(excludes.toArray(new Mutator[0]));

        // Get the result of the second query
        String queryString2 = CockroachDBVisitor.asString(select);
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

    private List<CockroachDBExpression> getJoins(List<CockroachDBExpression> tableList,
            CockroachDBGlobalState globalState) throws AssertionError {
        List<CockroachDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getPercentage() < 0.8) {
            CockroachDBTableReference leftTable = (CockroachDBTableReference) tableList.remove(0);
            CockroachDBTableReference rightTable = (CockroachDBTableReference) tableList.remove(0);
            List<CockroachDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            CockroachDBExpressionGenerator joinGen = new CockroachDBExpressionGenerator(globalState)
                    .setColumns(columns);
            joinExpressions.add(CockroachDBJoin.createJoin(leftTable, rightTable,
                    CockroachDBJoin.JoinType.getRandomExcept(JoinType.NATURAL),
                    joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
        }
        return joinExpressions;
    }

    @Override
    protected boolean mutateJoin() {
        if (select.getJoinList().isEmpty()) {
            return false;
        }

        CockroachDBJoin join = (CockroachDBJoin) Randomly.fromList(select.getJoinList());

        // CROSS does not need ON Condition, while other joins do
        // To avoid Null pointer, generating a new new condition when mutating CROSS to other joins
        if (join.getJoinType() == JoinType.CROSS) {
            List<CockroachDBColumn> columns = new ArrayList<>();
            columns.addAll(((CockroachDBTableReference) join.getLeftTable()).getTable().getColumns());
            columns.addAll(((CockroachDBTableReference) join.getRightTable()).getTable().getColumns());
            CockroachDBExpressionGenerator joinGen2 = new CockroachDBExpressionGenerator(state).setColumns(columns);
            join.setOnClause(joinGen2.generateExpression(CockroachDBDataType.BOOL.get()));
        }

        JoinType newJoinType = CockroachDBJoin.JoinType.INNER;
        if (join.getJoinType() == JoinType.LEFT || join.getJoinType() == JoinType.RIGHT) { // No invariant relation
                                                                                           // between LEFT and RIGHT
                                                                                           // join
            newJoinType = CockroachDBJoin.JoinType.getRandomExcept(JoinType.NATURAL, JoinType.CROSS, JoinType.LEFT,
                    JoinType.RIGHT);
        } else if (join.getJoinType() == JoinType.FULL) {
            newJoinType = CockroachDBJoin.JoinType.getRandomExcept(JoinType.NATURAL, JoinType.CROSS);
        } else if (join.getJoinType() != JoinType.CROSS) {
            newJoinType = CockroachDBJoin.JoinType.getRandomExcept(JoinType.NATURAL, join.getJoinType());
        }
        assert newJoinType != JoinType.NATURAL; // Natural Join is not supported for CERT
        boolean increase = join.getJoinType().ordinal() < newJoinType.ordinal();
        join.setJoinType(newJoinType);
        return increase;
    }

    @Override
    protected boolean mutateDistinct() {
        boolean increase = select.isDistinct();
        select.setDistinct(!select.isDistinct());
        return increase;
    }

    @Override
    protected boolean mutateWhere() {
        boolean increase = select.getWhereClause() != null;
        if (increase) {
            select.setWhereClause(null);
        } else {
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
        }
        return increase;
    }

    @Override
    protected boolean mutateGroupBy() {
        boolean increase = !select.getGroupByExpressions().isEmpty();
        if (increase) {
            select.clearGroupByExpressions();
        } else {
            select.setGroupByExpressions(select.getFetchColumns());
        }
        return increase;
    }

    @Override
    protected boolean mutateHaving() {
        if (select.getGroupByExpressions().isEmpty()) {
            select.setGroupByExpressions(select.getFetchColumns());
            select.setHavingClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
            return false;
        } else {
            if (select.getHavingClause() == null) {
                select.setHavingClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
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
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
        } else {
            CockroachDBExpression newWhere = new CockroachDBBinaryLogicalOperation(select.getWhereClause(),
                    gen.generateExpression(CockroachDBDataType.BOOL.get()), CockroachDBBinaryLogicalOperator.AND);
            select.setWhereClause(newWhere);
        }
        return false;
    }

    @Override
    protected boolean mutateOr() {
        if (select.getWhereClause() == null) {
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
            return false;
        } else {
            CockroachDBExpression newWhere = new CockroachDBBinaryLogicalOperation(select.getWhereClause(),
                    gen.generateExpression(CockroachDBDataType.BOOL.get()), CockroachDBBinaryLogicalOperator.OR);
            select.setWhereClause(newWhere);
            return true;
        }
    }

    @Override
    protected boolean mutateLimit() {
        boolean increase = select.getLimitClause() != null;
        if (increase) {
            select.setLimitClause(null);
        } else {
            select.setLimitClause(gen.generateConstant(CockroachDBDataType.INT.get()));
        }
        return increase;
    }

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
                    String content = rs.getString(1);
                    if (content.contains("count:")) {
                        try {
                            int number = Integer.parseInt(content.split("count: ")[1].split(" ")[0].replace(",", ""));
                            if (row == -1) {
                                row = number;
                            }
                        } catch (Exception e) { // To avoid the situation that no number is found
                        }
                    }
                    if (content.contains("• ")) {
                        String operation = content.split("• ")[1].split(" ")[0];
                        if (CockroachDBBugs.bug131875 && (operation.equals("distinct") || operation.equals("limit"))) {
                            throw new IgnoreMeException();
                        }
                        queryPlanSequences.add(operation);
                    }
                }
            }
        } catch (IgnoreMeException e) {
            throw new IgnoreMeException();
        } catch (Exception e) {
            throw new AssertionError(q.getQueryString(), e);
        }
        if (row == -1) {
            throw new IgnoreMeException();
        }
        return row;
    }

}
