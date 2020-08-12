package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBJoin.OuterType;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

public class CockroachDBTLPJoinOracle implements TestOracle {

    final CockroachDBGlobalState state;
    final ExpectedErrors errors = new ExpectedErrors();

    CockroachDBSchema s;
    CockroachDBTables targetTables;
    CockroachDBExpressionGenerator gen;
    CockroachDBSelect select;
    CockroachDBExpression predicate;
    CockroachDBExpression negatedPredicate;
    CockroachDBExpression isNullPredicate;

    public CockroachDBTLPJoinOracle(CockroachDBGlobalState state) {
        errors.add("GROUP BY term out of range");
        CockroachDBErrors.addExpressionErrors(errors);
        this.state = state;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        select = new CockroachDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<CockroachDBTable> tables = targetTables.getTables();
        if (tables.size() < 2) {
            return;
        }
        List<CockroachDBExpression> tableList = tables.stream().map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());
        CockroachDBTableReference leftTable = (CockroachDBTableReference) tableList.remove(0);
        CockroachDBTableReference rightTable = (CockroachDBTableReference) tableList.remove(0);
        CockroachDBJoin leftJoinTrue = CockroachDBJoin.createOuterJoin(leftTable, rightTable, OuterType.LEFT,
                CockroachDBConstant.createBooleanConstant(true));

        select.setJoinList(Arrays.asList(leftJoinTrue));
        select.setFromList(tableList);
        select.setWhereClause(null);
        predicate = generatePredicate();
        negatedPredicate = new CockroachDBNotOperation(predicate);
        isNullPredicate = new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL);

        String originalQueryString1 = CockroachDBVisitor.asString(select);

        CockroachDBJoin leftJoinFalse = CockroachDBJoin.createOuterJoin(leftTable, rightTable, OuterType.LEFT,
                CockroachDBConstant.createBooleanConstant(false));
        select.setJoinList(Arrays.asList(leftJoinFalse));
        String originalQueryString2 = CockroachDBVisitor.asString(select);
        String originalQueryString = originalQueryString1 + " UNION ALL " + originalQueryString2 + " UNION ALL "
                + originalQueryString2;

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // boolean allowOrderBy = Randomly.getBoolean();
        // if (allowOrderBy) {
        // select.setOrderByExpressions(gen.getOrderingTerms());
        // }

        select.setJoinList(
                Arrays.asList(CockroachDBJoin.createOuterJoin(leftTable, rightTable, OuterType.LEFT, predicate)));
        String firstQueryString = CockroachDBVisitor.asString(select);

        select.setJoinList(Arrays
                .asList(CockroachDBJoin.createOuterJoin(leftTable, rightTable, OuterType.LEFT, negatedPredicate)));
        String secondQueryString = CockroachDBVisitor.asString(select);

        select.setJoinList(
                Arrays.asList(CockroachDBJoin.createOuterJoin(leftTable, rightTable, OuterType.LEFT, isNullPredicate)));
        String thirdQueryString = CockroachDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    List<CockroachDBExpression> generateFetchColumns() {
        return Arrays.asList(new CockroachDBColumnReference(targetTables.getColumns().get(0)));
    }

    CockroachDBExpression generatePredicate() {
        return gen.generateExpression(CockroachDBDataType.BOOL.get());
    }

}
