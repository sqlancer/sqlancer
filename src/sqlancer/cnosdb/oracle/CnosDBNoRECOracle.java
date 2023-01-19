package sqlancer.cnosdb.oracle;

import java.util.ArrayList;
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
import sqlancer.cnosdb.ast.CnosDBJoin.CnosDBJoinType;
import sqlancer.cnosdb.ast.CnosDBPostfixText;
import sqlancer.cnosdb.ast.CnosDBSelect;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBFromTable;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBSubquery;
import sqlancer.cnosdb.ast.CnosDBSelect.SelectType;
import sqlancer.cnosdb.client.CnosDBResultSet;
import sqlancer.cnosdb.gen.CnosDBExpressionGenerator;
import sqlancer.cnosdb.oracle.tlp.CnosDBTLPBase;
import sqlancer.cnosdb.query.CnosDBSelectQuery;
import sqlancer.common.oracle.TestOracle;

public class CnosDBNoRECOracle extends CnosDBNoRECBase implements TestOracle<CnosDBGlobalState> {

    private final CnosDBSchema s;

    public CnosDBNoRECOracle(CnosDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
    }

    public static List<CnosDBJoin> getJoinStatements(CnosDBGlobalState globalState, List<CnosDBColumn> columns,
            List<CnosDBTable> tables) {
        List<CnosDBJoin> joinStatements = new ArrayList<>();
        CnosDBExpressionGenerator gen = new CnosDBExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            CnosDBExpression joinClause = gen.generateExpression(CnosDBDataType.BOOLEAN);
            CnosDBTable table = Randomly.fromList(tables);
            tables.remove(table);
            CnosDBJoinType options = CnosDBJoinType.getRandom();
            CnosDBJoin j = new CnosDBJoin(new CnosDBFromTable(table), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            CnosDBTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            CnosDBSubquery subquery = CnosDBTLPBase.createSubquery(globalState, String.format("sub%d", i),
                    subqueryTables);
            CnosDBExpression joinClause = gen.generateExpression(CnosDBDataType.BOOLEAN);
            CnosDBJoinType options = CnosDBJoinType.getRandom();
            CnosDBJoin j = new CnosDBJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    @Override
    public void check() throws Exception {
        CnosDBTables randomTables = s.getRandomTableNonEmptyTables();
        List<CnosDBColumn> columns = randomTables.getColumns();
        CnosDBExpression randomWhereCondition = getRandomWhereCondition(columns);
        List<CnosDBTable> tables = randomTables.getTables();

        List<CnosDBJoin> joinStatements = getJoinStatements(state, columns, tables);
        List<CnosDBExpression> fromTables = tables.stream().map(CnosDBFromTable::new).collect(Collectors.toList());
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

    private CnosDBExpression getRandomWhereCondition(List<CnosDBColumn> columns) {
        return new CnosDBExpressionGenerator(state).setColumns(columns).generateExpression(CnosDBDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<CnosDBExpression> fromTables, CnosDBExpression randomWhereCondition,
            List<CnosDBJoin> joinStatements) throws Exception {
        CnosDBSelect select = new CnosDBSelect();
        CnosDBCastOperation isTrue = new CnosDBCastOperation(randomWhereCondition,
                CnosDBCompoundDataType.create(CnosDBDataType.INT));
        CnosDBPostfixText asText = new CnosDBPostfixText(isTrue, " as count", null, CnosDBDataType.INT);
        select.setFetchColumns(List.of(asText));
        select.setFromList(fromTables);
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + CnosDBVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        CnosDBSelectQuery q = new CnosDBSelectQuery(unoptimizedQueryString, errors);
        CnosDBResultSet rs;
        errors.addAll(CnosDBExpectedError.expectedErrors());
        try {
            q.executeAndGet(state);
            rs = q.getResultSet();
        } catch (Exception e) {
            if (errors.errorIsExpected(e.getMessage())) {
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

    private int getOptimizedQueryCount(List<CnosDBExpression> randomTables, List<CnosDBColumn> columns,
            CnosDBExpression randomWhereCondition, List<CnosDBJoin> joinStatements) {
        CnosDBSelect select = new CnosDBSelect();
        CnosDBColumnValue allColumns = new CnosDBColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(List.of(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new CnosDBExpressionGenerator(state).setColumns(columns).generateOrderBy());
        }
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int firstCount = 0;
        optimizedQueryString = CnosDBVisitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(optimizedQueryString);
        }
        errors.addAll(CnosDBExpectedError.expectedErrors());
        CnosDBSelectQuery query = new CnosDBSelectQuery(optimizedQueryString, errors);
        CnosDBResultSet rs;
        try {
            query.executeAndGet(state);
            rs = query.getResultSet();
            while (rs.next()) {
                firstCount++;
            }
        } catch (Exception e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }

            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
