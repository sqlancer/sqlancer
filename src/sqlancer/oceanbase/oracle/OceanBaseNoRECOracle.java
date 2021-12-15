package sqlancer.oceanbase.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseVisitor;
import sqlancer.oceanbase.ast.*;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;
import sqlancer.oceanbase.ast.OceanBaseComputableFunction;
import sqlancer.oceanbase.ast.OceanBaseComputableFunction.OceanBaseFunction;
import sqlancer.oceanbase.ast.OceanBaseConstant;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation;
import sqlancer.oceanbase.ast.OceanBaseBinaryLogicalOperation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Random;

public class OceanBaseNoRECOracle extends NoRECBase<OceanBaseGlobalState>  implements TestOracle  {

    // SELECT COUNT(*) FROM t0 WHERE <cond>;
    // SELECT SUM(count) FROM (SELECT <cond> IS TRUE as count FROM t0);
    // SELECT (SELECT COUNT(*) FROM t0 WHERE c0 IS NOT 0) = (SELECT COUNT(*) FROM
    // (SELECT c0 is NOT 0 FROM t0));
    private OceanBaseSchema s;
    private Randomly r;
    private String firstQueryString;
    private String secondQueryString;
    private static final int NOT_FOUND = -1;

    public OceanBaseNoRECOracle(OceanBaseGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        errors.add("is out of range");
        // regex
        errors.add("unmatched parentheses");
        errors.add("nothing to repeat at offset");
        errors.add("missing )");
        errors.add("missing terminating ]");
        errors.add("range out of order in character class");
        errors.add("unrecognized character after ");
        errors.add("Got error '(*VERB) not recognized or malformed");
        errors.add("must be followed by");
        errors.add("malformed number or name after");
        errors.add("digit expected after");
    }

    public void check() throws SQLException {
        OceanBaseSchema.OceanBaseTable randomTable = s.getRandomTable();
        List<OceanBaseSchema.OceanBaseColumn> columns = randomTable.getColumns();
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(state).setColumns(columns);
        OceanBaseExpression randomWhereCondition = gen.generateExpression();
        List<OceanBaseExpression> groupBys = Collections.emptyList(); // getRandomExpressions(columns);
        List<OceanBaseExpression> tableList = (Arrays.asList(randomTable)).stream().map(t -> new OceanBaseTableReference(t))
                .collect(Collectors.toList());
        int firstCount = getFirstQueryCount(tableList, randomWhereCondition, groupBys);
        int secondCount = getSecondQuery(tableList, randomWhereCondition, groupBys);
        if (firstCount != secondCount && firstCount != NOT_FOUND && secondCount != NOT_FOUND) {
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

    private int getSecondQuery(List<OceanBaseExpression> tableList, OceanBaseExpression randomWhereCondition,
                               List<OceanBaseExpression> groupBys) throws SQLException {
        OceanBaseSelect select = new OceanBaseSelect();
        select.setGroupByClause(groupBys);
        OceanBaseExpression expr = getTrueExpr(randomWhereCondition);

        OceanBaseText asText = new OceanBaseText(expr, " as count", false);
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(tableList);
        select.setSelectType(OceanBaseSelect.SelectType.ALL);
        int secondCount = 0;

        unoptimizedQueryString = "SELECT SUM(count) FROM (" + OceanBaseVisitor.asString(select) + ") as asdf";
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(optimizedQueryString, e);
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

    private int getFirstQueryCount(List<OceanBaseExpression> tableList, OceanBaseExpression randomWhereCondition,
            List<OceanBaseExpression> groupBys) throws SQLException {
        OceanBaseSelect select = new OceanBaseSelect();
        select.setGroupByClause(groupBys);
        // SELECT COUNT(t1.c3) FROM t1 WHERE (- (t1.c2));
        // SELECT SUM(count) FROM (SELECT ((- (t1.c2)) IS TRUE) as count FROM t1);;
        OceanBaseAggregate aggr = new OceanBaseAggregate(
                new OceanBaseColumnName(new OceanBaseSchema.OceanBaseColumn("*", OceanBaseSchema.OceanBaseDataType.INT, false, 0, false)),
                OceanBaseAggregate.OceanBaseAggregateFunction.COUNT);
        select.setFetchColumns(Arrays.asList(aggr));
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        select.setSelectType(OceanBaseSelect.SelectType.ALL);
        int firstCount = 0;
        optimizedQueryString = OceanBaseVisitor.asString(select);
        SQLQueryAdapter q = new SQLQueryAdapter(optimizedQueryString, errors);
        SQLancerResultSet rs;
        if (options.logEachSelect()) {
            logger.writeCurrent(optimizedQueryString);
        }
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(firstQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            firstCount += rs.getLong(1);
        }
        rs.close();
        return firstCount;
    }
  
    private OceanBaseExpression getTrueExpr(OceanBaseExpression randomWhereCondition){
        int i = new Random().nextInt(8);
        OceanBaseUnaryPostfixOperation isTrue = new OceanBaseUnaryPostfixOperation(randomWhereCondition,OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_TRUE,false);

        OceanBaseUnaryPostfixOperation isNotTrue = new OceanBaseUnaryPostfixOperation(randomWhereCondition,OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_TRUE,true);

        OceanBaseUnaryPostfixOperation isFalse= new OceanBaseUnaryPostfixOperation(randomWhereCondition,OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_FALSE,false);

        OceanBaseUnaryPostfixOperation isNotFalse= new OceanBaseUnaryPostfixOperation(randomWhereCondition,OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_FALSE,true);

        OceanBaseUnaryPostfixOperation isNULL= new OceanBaseUnaryPostfixOperation(randomWhereCondition,OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL,false);

        OceanBaseUnaryPostfixOperation isNotNULL= new OceanBaseUnaryPostfixOperation(randomWhereCondition,OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL,true);

        OceanBaseExpression expr = OceanBaseConstant.createNullConstant(); 
        switch(i){
           case 0:        
               expr = isTrue;
               break; 
           case 1:        
               expr = new OceanBaseUnaryPrefixOperation(new OceanBaseBinaryLogicalOperation(isFalse, isNULL, OceanBaseBinaryLogicalOperation.OceanBaseBinaryLogicalOperator.OR), OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator.NOT);
               break; 
           case 2:
               expr = new OceanBaseUnaryPrefixOperation(new OceanBaseUnaryPrefixOperation(isTrue, OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator.NOT), OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator.NOT);
               break;
           case 3:
               expr = new OceanBaseUnaryPrefixOperation(isNotTrue, OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator.NOT);
               break;
           case 4:
               expr = new OceanBaseBinaryLogicalOperation(isNotFalse, isNotNULL, OceanBaseBinaryLogicalOperation.OceanBaseBinaryLogicalOperator.AND);
               break;
           case 5:
               OceanBaseExpression[] args = new OceanBaseExpression[3];
               args[0] = OceanBaseConstant.createIntConstant(1);
               args[1] = isTrue;
               args[2] = OceanBaseConstant.createIntConstant(0);
               expr = new OceanBaseComputableFunction(OceanBaseFunction.IF, args);
               break; 
           case 6:
               OceanBaseExpression[] ifArgs = new OceanBaseExpression[2];
               ifArgs[0] = OceanBaseConstant.createNullConstant();
               ifArgs[1] = isTrue;
               expr = new OceanBaseComputableFunction(OceanBaseFunction.IFNULL, ifArgs);
               break; 
           case 7:
               OceanBaseExpression[] coalesceArgs = new OceanBaseExpression[2];
               coalesceArgs[0] = OceanBaseConstant.createNullConstant();
               coalesceArgs[1] = isTrue;
               expr = new OceanBaseComputableFunction(OceanBaseFunction.COALESCE, coalesceArgs);
               break; 
        }
        return expr;
    }
       

}
