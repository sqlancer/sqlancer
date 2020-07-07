package sqlancer.cockroachdb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBJoin.OuterType;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;

public class CockroachDBNoRECOracle implements TestOracle {

    private final CockroachDBGlobalState globalState;
    private final Set<String> errors = new HashSet<>();
    private String optimizableQueryString;
    private String unoptimizedQuery;
    private CockroachDBExpressionGenerator gen;

    public CockroachDBNoRECOracle(CockroachDBGlobalState globalState) {
        this.globalState = globalState;
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("unable to vectorize execution plan"); // SET vectorize=experimental_always;
        errors.add(" mismatched physical types at index"); // SET vectorize=experimental_always;

    }

    @Override
    public void check() throws SQLException {
        CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<CockroachDBTableReference> tableL = tables.getTables().stream().map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());
        List<CockroachDBExpression> tableList = CockroachDBCommon.getTableReferences(tableL);
        gen = new CockroachDBExpressionGenerator(globalState).setColumns(tables.getColumns());
        List<CockroachDBExpression> joinExpressions = getJoins(tableList, globalState);
        CockroachDBExpression whereCondition = gen.generateExpression(CockroachDBDataType.BOOL.get());
        int optimizableCount = getOptimizedResult(whereCondition, tableList, errors, joinExpressions);
        if (optimizableCount == -1) {
            throw new IgnoreMeException();
        }
        int nonOptimizableCount = getNonOptimizedResult(whereCondition, tableList, errors, joinExpressions);
        if (nonOptimizableCount == -1) {
            throw new IgnoreMeException();
        }
        if (optimizableCount != nonOptimizableCount) {
            globalState.getState().queryString = optimizableQueryString + ";\n" + unoptimizedQuery + ";";
            throw new AssertionError(CockroachDBVisitor.asString(whereCondition));
        }
    }

    public static List<CockroachDBExpression> getJoins(List<CockroachDBExpression> tableList,
            CockroachDBGlobalState globalState) throws AssertionError {
        List<CockroachDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBoolean()) {
            CockroachDBTableReference leftTable = (CockroachDBTableReference) tableList.remove(0);
            CockroachDBTableReference rightTable = (CockroachDBTableReference) tableList.remove(0);
            List<CockroachDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            CockroachDBExpressionGenerator joinGen = new CockroachDBExpressionGenerator(globalState)
                    .setColumns(columns);
            switch (CockroachDBJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(new CockroachDBJoin(leftTable, rightTable, CockroachDBJoin.JoinType.INNER,
                        joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
                break;
            case NATURAL:
                joinExpressions.add(CockroachDBJoin.createNaturalJoin(leftTable, rightTable));
                break;
            case CROSS:
                joinExpressions.add(CockroachDBJoin.createCrossJoin(leftTable, rightTable));
                break;
            case OUTER:
                joinExpressions.add(CockroachDBJoin.createOuterJoin(leftTable, rightTable, OuterType.getRandom(),
                        joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    private int getOptimizedResult(CockroachDBExpression whereCondition, List<CockroachDBExpression> tableList,
            Set<String> errors, List<CockroachDBExpression> joinExpressions) throws SQLException {
        CockroachDBSelect select = new CockroachDBSelect();
        CockroachDBColumn c = new CockroachDBColumn("COUNT(*)", null, false, false);
        select.setFetchColumns(Arrays.asList(new CockroachDBColumnReference(c)));
        select.setFromList(tableList);
        select.setWhereClause(whereCondition);
        select.setJoinList(joinExpressions);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.getOrderingTerms());
        }
        String s = CockroachDBVisitor.asString(select);
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(s);
        }
        this.optimizableQueryString = s;
        Query q = new QueryAdapter(s, errors);
        return getCount(globalState, q);
    }

    private int getNonOptimizedResult(CockroachDBExpression whereCondition, List<CockroachDBExpression> tableList,
            Set<String> errors, List<CockroachDBExpression> joinList) throws SQLException {
        String fromString = tableList.stream().map(t -> ((CockroachDBTableReference) t).getTable().getName())
                .collect(Collectors.joining(", "));
        if (!tableList.isEmpty() && !joinList.isEmpty()) {
            fromString += ", ";
        }
        String s = "SELECT SUM(count) FROM (SELECT CAST(" + CockroachDBVisitor.asString(whereCondition)
                + " IS TRUE AS INT) as count FROM " + fromString + " "
                + joinList.stream().map(j -> CockroachDBVisitor.asString(j)).collect(Collectors.joining(", ")) + ")";
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(s);
        }
        this.unoptimizedQuery = s;
        Query q = new QueryAdapter(s, errors);
        return getCount(globalState, q);
    }

    private int getCount(GlobalState<?> globalState, Query q) throws AssertionError {
        int count = 0;
        try (ResultSet rs = q.executeAndGet(globalState)) {
            if (rs == null) {
                return -1;
            }
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (Exception e) {
            throw new AssertionError(q.getQueryString(), e);
        }
        return count;
    }

}
