package sqlancer.tidb.oracle;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.ast.TiDBBinaryLogicalOperation;
import sqlancer.tidb.ast.TiDBBinaryLogicalOperation.TiDBBinaryLogicalOperator;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBJoin.JoinType;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBCERTOracle implements TestOracle<TiDBGlobalState> {
    private final TiDBGlobalState state;
    private TiDBExpressionGenerator gen;
    private final ExpectedErrors errors = new ExpectedErrors();
    private TiDBSelect select;
    private List<String> queryPlan1Sequences;
    private List<String> queryPlan2Sequences;

    public enum Mutator {
        JOIN, WHERE, GROUPBY, HAVING, AND, OR, LIMIT;

        public static Mutator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public TiDBCERTOracle(TiDBGlobalState globalState) {
        state = globalState;
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        queryPlan1Sequences = new ArrayList<>();
        queryPlan2Sequences = new ArrayList<>();

        // Randomly generate a query
        TiDBTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new TiDBExpressionGenerator(state).setColumns(tables.getColumns());
        select = new TiDBSelect();

        List<TiDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream().map(c -> new TiDBColumnReference(c))
                .collect(Collectors.toList()));
        select.setFetchColumns(fetchColumns);

        List<TiDBExpression> tableList = tables.getTables().stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        List<TiDBExpression> joins = TiDBJoin.getJoinsWithoutNature(tableList, state);
        select.setJoinList(joins);
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(select.getFetchColumns());
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(gen.generateExpression());
        }

        // Get the result of the first query
        String queryString1 = TiDBVisitor.asString(select);
        double rowCount1 = getRow(state, queryString1, queryPlan1Sequences);

        // Mutate the query
        boolean increase = false;

        Mutator mutation = Mutator.getRandom();
        switch (mutation) {
        case JOIN:
            increase = mutateJoin();
            break;
        case WHERE:
            increase = mutateWhere();
            break;
        case GROUPBY:
            increase = mutateGroupBy();
            break;
        case HAVING:
            increase = mutateHaving();
            break;
        case AND:
            increase = mutateAnd();
            break;
        case OR:
            increase = mutateOr();
            break;
        case LIMIT:
            increase = mutateLimit();
            break;
        default:
            throw new AssertionError();
        }

        // Get the result of the second query
        String queryString2 = TiDBVisitor.asString(select);
        double rowCount2 = getRow(state, queryString2, queryPlan2Sequences);

        // Check structural equivalence
        if (!DBMSCommon.areQueryPlanSequencesSimilar(queryPlan1Sequences, queryPlan2Sequences)) {
            return;
        }

        /*
         * https://github.com/pingcap/tidb/issues/38474 A minor issue in TiDB that some operations would round the
         * result while others would not. A false alarm happens when both queries have the same number of estimated rows
         * but the restrictued one rounds up to a bigger number. To avoid this issue until it is fixed, we make sure the
         * gap between two estimated rows is at least 1.
         */
        // Check the results
        if (increase && rowCount1 > (rowCount2 + 1) || !increase && (rowCount1 + 1) < rowCount2) {
            throw new AssertionError("Mutator: " + mutation + ", Inconsistent result for query: EXPLAIN " + queryString1
                    + "; --" + rowCount1 + "\nEXPLAIN " + queryString2 + "; --" + rowCount2);
        }
    }

    private boolean mutateJoin() {
        if (select.getJoinList().isEmpty()) {
            return false;
        }
        TiDBJoin join = (TiDBJoin) Randomly.fromList(select.getJoinList());

        // CROSS does not need ON Condition, while other joins do
        // To avoid Null pointer, generating a new new condition when mutating CROSS to other joins
        if (join.getJoinType() == JoinType.CROSS) {
            List<TiDBColumn> columns = new ArrayList<>();
            columns.addAll(((TiDBTableReference) join.getLeftTable()).getTable().getColumns());
            columns.addAll(((TiDBTableReference) join.getRightTable()).getTable().getColumns());
            TiDBExpressionGenerator joinGen2 = new TiDBExpressionGenerator(state).setColumns(columns);
            join.setOnCondition(joinGen2.generateExpression());
        }

        JoinType newJoinType = TiDBJoin.JoinType.INNER;
        if (join.getJoinType() == JoinType.LEFT || join.getJoinType() == JoinType.RIGHT) { // No invarient relation
                                                                                           // between LEFT and RIGHT
                                                                                           // join
            newJoinType = Randomly.fromOptions(Arrays.stream(JoinType.values())
                    .filter(j -> j != JoinType.LEFT && j != JoinType.RIGHT && j != JoinType.NATURAL)
                    .toArray(JoinType[]::new));
        } else {
            newJoinType = Randomly.fromOptions(Arrays.stream(JoinType.values())
                    .filter(j -> j != join.getJoinType() && j != JoinType.NATURAL).toArray(JoinType[]::new));
        }

        boolean increase = join.getJoinType().ordinal() < newJoinType.ordinal();
        join.setJoinType(newJoinType);
        if (newJoinType == JoinType.CROSS) {
            join.setOnCondition(null);
        }
        return increase;
    }

    private boolean mutateWhere() {
        boolean increase = select.getWhereClause() != null;
        if (increase) {
            select.setWhereClause(null);
        } else {
            select.setWhereClause(gen.generateExpression());
        }
        return increase;
    }

    private boolean mutateGroupBy() {
        boolean increase = select.getGroupByExpressions().size() > 0;
        if (increase) {
            select.clearGroupByExpressions();
            select.clearHavingClause();
        } else {
            select.setGroupByExpressions(select.getFetchColumns());
        }
        return increase;
    }

    private boolean mutateHaving() {
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

    private boolean mutateAnd() {
        if (select.getWhereClause() == null) {
            select.setWhereClause(gen.generateExpression());
        } else {
            TiDBExpression newWhere = new TiDBBinaryLogicalOperation(select.getWhereClause(), gen.generateExpression(),
                    TiDBBinaryLogicalOperator.AND);
            select.setWhereClause(newWhere);
        }
        return false;
    }

    private boolean mutateOr() {
        if (select.getWhereClause() == null) {
            select.setWhereClause(gen.generateExpression());
            return false;
        } else {
            TiDBExpression newWhere = new TiDBBinaryLogicalOperation(select.getWhereClause(), gen.generateExpression(),
                    TiDBBinaryLogicalOperator.OR);
            select.setWhereClause(newWhere);
            return true;
        }
    }

    private boolean mutateLimit() {
        boolean increase = select.getLimitClause() != null;
        if (increase) {
            select.setLimitClause(null);
        } else {
            select.setLimitClause(gen.generateConstant(TiDBDataType.INT));
        }
        return increase;
    }

    private double getRow(SQLGlobalState<?, ?> globalState, String selectStr, List<String> queryPlanSequences)
            throws AssertionError, SQLException {
        double row = -1;
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
                    double estRows = Double.parseDouble(rs.getString(2));
                    if (row == -1) {
                        row = estRows;
                    }
                    String operation = rs.getString(1).split("_")[0]; // Extract operation names for query plans
                    queryPlanSequences.add(operation);
                    return estRows;
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
