package sqlancer.materialize.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.materialize.MaterializeCompoundDataType;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeSchema.MaterializeTables;
import sqlancer.materialize.MaterializeVisitor;
import sqlancer.materialize.ast.MaterializeCastOperation;
import sqlancer.materialize.ast.MaterializeColumnValue;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeJoin;
import sqlancer.materialize.ast.MaterializeJoin.MaterializeJoinType;
import sqlancer.materialize.ast.MaterializePostfixText;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeFromTable;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeSubquery;
import sqlancer.materialize.ast.MaterializeSelect.SelectType;
import sqlancer.materialize.gen.MaterializeCommon;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;
import sqlancer.materialize.oracle.tlp.MaterializeTLPBase;

public class MaterializeNoRECOracle extends NoRECBase<MaterializeGlobalState>
        implements TestOracle<MaterializeGlobalState> {

    private final MaterializeSchema s;

    public MaterializeNoRECOracle(MaterializeGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        MaterializeCommon.addCommonExpressionErrors(errors);
        MaterializeCommon.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        MaterializeTables randomTables = s.getRandomTableNonEmptyTables();
        List<MaterializeColumn> columns = randomTables.getColumns();
        MaterializeExpression randomWhereCondition = getRandomWhereCondition(columns);
        List<MaterializeTable> tables = randomTables.getTables();

        List<MaterializeJoin> joinStatements = getJoinStatements(state, columns, tables);
        List<MaterializeExpression> fromTables = tables.stream()
                .map(t -> new MaterializeFromTable(t, Randomly.getBoolean())).collect(Collectors.toList());
        int secondCount = getUnoptimizedQueryCount(fromTables, randomWhereCondition, joinStatements);
        int firstCount = getOptimizedQueryCount(fromTables, columns, randomWhereCondition, joinStatements);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            String queryFormatString = "-- %s;\n-- count: %d";
            String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, firstCount);
            String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString, secondCount);
            state.getState().getLocalState()
                    .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
            String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", firstCount, secondCount,
                    firstQueryStringWithCount, secondQueryStringWithCount);
            throw new AssertionError(assertionMessage);
        }
    }

    public static List<MaterializeJoin> getJoinStatements(MaterializeGlobalState globalState,
            List<MaterializeColumn> columns, List<MaterializeTable> tables) {
        List<MaterializeJoin> joinStatements = new ArrayList<>();
        MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            MaterializeExpression joinClause = gen.generateExpression(MaterializeDataType.BOOLEAN);
            MaterializeTable table = Randomly.fromList(tables);
            tables.remove(table);
            MaterializeJoinType options = MaterializeJoinType.getRandom();
            MaterializeJoin j = new MaterializeJoin(new MaterializeFromTable(table, Randomly.getBoolean()), joinClause,
                    options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            MaterializeTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            MaterializeSubquery subquery = MaterializeTLPBase.createSubquery(globalState, String.format("sub%d", i),
                    subqueryTables);
            MaterializeExpression joinClause = gen.generateExpression(MaterializeDataType.BOOLEAN);
            MaterializeJoinType options = MaterializeJoinType.getRandom();
            MaterializeJoin j = new MaterializeJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    private MaterializeExpression getRandomWhereCondition(List<MaterializeColumn> columns) {
        return new MaterializeExpressionGenerator(state).setColumns(columns)
                .generateExpression(MaterializeDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<MaterializeExpression> fromTables,
            MaterializeExpression randomWhereCondition, List<MaterializeJoin> joinStatements) throws SQLException {
        MaterializeSelect select = new MaterializeSelect();
        MaterializeCastOperation isTrue = new MaterializeCastOperation(randomWhereCondition,
                MaterializeCompoundDataType.create(MaterializeDataType.INT));
        MaterializePostfixText asText = new MaterializePostfixText(isTrue, " as count", null, MaterializeDataType.INT);
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(fromTables);
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + MaterializeVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(List<MaterializeExpression> randomTables, List<MaterializeColumn> columns,
            MaterializeExpression randomWhereCondition, List<MaterializeJoin> joinStatements) throws SQLException {
        MaterializeSelect select = new MaterializeSelect();
        MaterializeColumnValue allColumns = new MaterializeColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(Arrays.asList(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(
                    new MaterializeExpressionGenerator(state).setColumns(columns).generateOrderBy());
        }
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = MaterializeVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

    @Override
    public String getLastQueryString() {
        return optimizedQueryString;
    }
}
