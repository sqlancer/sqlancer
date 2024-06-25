package sqlancer.cnosdb.oracle;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBCompoundDataType;
import sqlancer.cnosdb.CnosDBExpectedError;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTables;
import sqlancer.cnosdb.CnosDBVisitor;
import sqlancer.cnosdb.ast.CnosDBCastOperation;
import sqlancer.cnosdb.ast.CnosDBColumnValue;
import sqlancer.cnosdb.ast.CnosDBExpression;
import sqlancer.cnosdb.ast.CnosDBJoin;
// import sqlancer.cnosdb.ast.CnosDBJoin.CnosDBJoinType;
import sqlancer.cnosdb.ast.CnosDBPostfixText;
import sqlancer.cnosdb.ast.CnosDBSelect;
import sqlancer.cnosdb.ast.CnosDBTableReference;
import sqlancer.cnosdb.ast.CnosDBSelect.SelectType;
import sqlancer.cnosdb.client.CnosDBResultSet;
import sqlancer.cnosdb.gen.CnosDBExpressionGenerator;
import sqlancer.cnosdb.query.CnosDBSelectQuery;
import sqlancer.common.oracle.TestOracle;

public class CnosDBNoRECOracle extends CnosDBNoRECBase implements TestOracle<CnosDBGlobalState> {

    private final CnosDBSchema s;

    public CnosDBNoRECOracle(CnosDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
    }

    @Override
    public void check() throws Exception {
        CnosDBTables randomTables = s.getRandomTableNonEmptyTables();
        List<CnosDBColumn> columns = randomTables.getColumns();

        CnosDBExpressionGenerator gen = new CnosDBExpressionGenerator(state).setColumns(columns);
        CnosDBExpression randomWhereCondition = gen.generateExpression(CnosDBDataType.BOOLEAN);

        List<CnosDBTable> tables = randomTables.getTables();

        List<CnosDBExpression> tableList = tables.stream().map(t -> new CnosDBTableReference(t)).collect(Collectors.toList());
        List<CnosDBExpression> joins = CnosDBJoin.getJoins(tableList, state);
        int secondCount = getUnoptimizedQueryCount(tableList, randomWhereCondition, joins);
        int firstCount = getOptimizedQueryCount(tableList, List.of(CnosDBColumn.createDummy("f0")),
                randomWhereCondition, joins);
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


    private int getUnoptimizedQueryCount(List<CnosDBExpression> tableList, CnosDBExpression randomWhereCondition,
            List<CnosDBExpression> joinStatements) throws Exception {
        CnosDBSelect select = new CnosDBSelect();
        CnosDBCastOperation isTrue = new CnosDBCastOperation(randomWhereCondition,
                CnosDBCompoundDataType.create(CnosDBDataType.INT));
        CnosDBPostfixText asText = new CnosDBPostfixText(isTrue, " as count", CnosDBDataType.INT);
        select.setFetchColumns(List.of(asText));
        select.setFromList(tableList);
        select.setSelectType(SelectType.ALL);
        select.setJoinList(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + CnosDBVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        CnosDBSelectQuery q = new CnosDBSelectQuery(unoptimizedQueryString, CnosDBExpectedError.expectedErrors());
        CnosDBResultSet rs;
        try {
            q.executeAndGet(state);
            rs = q.getResultSet();
        } catch (Exception e) {
            if (q.getExpectedErrors().errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
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

    private int getOptimizedQueryCount(List<CnosDBExpression> tableLists, List<CnosDBColumn> columns,
            CnosDBExpression randomWhereCondition, List<CnosDBExpression> joinStatements) {
        CnosDBSelect select = new CnosDBSelect();
        CnosDBColumnValue allColumns = new CnosDBColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(List.of(allColumns));
        select.setFromList(tableLists);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByClauses(new CnosDBExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        select.setSelectType(SelectType.ALL);
        select.setJoinList(joinStatements);
        int firstCount = 0;
        optimizedQueryString = CnosDBVisitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(optimizedQueryString);
        }
        CnosDBSelectQuery query = new CnosDBSelectQuery(optimizedQueryString, CnosDBExpectedError.expectedErrors());
        CnosDBResultSet rs;
        try {
            query.executeAndGet(state);
            rs = query.getResultSet();
            while (rs.next()) {
                firstCount++;
            }
        } catch (Exception e) {
            if (query.getExpectedErrors().errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }

            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
