package sqlancer.clickhouse.ast;

import org.junit.jupiter.api.Test;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.constant.ClickHouseCreateConstant;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseOperatorsVisitorTest {

    @Test
    void selectUnaryNot() {
        ClickHouseConstant trueConst = ClickHouseCreateConstant.createBoolean(true);
        ClickHouseExpression notTrue = new ClickHouseUnaryPrefixOperation(trueConst,
                ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator.NOT);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(notTrue));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT (NOT (true))";
        assertEquals(answer, result);
    }

    @Test
    void selectUnaryMinus() {
        ClickHouseConstant fiveConst = ClickHouseCreateConstant.createUInt32Constant(5);
        ClickHouseExpression minusFive = new ClickHouseUnaryPrefixOperation(fiveConst,
                ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator.MINUS);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(minusFive));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT (- (5))";
        assertEquals(answer, result);
    }

    @Test
    void selectUnaryExp() {
        ClickHouseConstant tenConst = ClickHouseCreateConstant.createInt32Constant(10);
        ClickHouseExpression minusFive = new ClickHouseUnaryFunctionOperation(tenConst,
                ClickHouseUnaryFunctionOperation.ClickHouseUnaryFunctionOperator.EXP);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(minusFive));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT (exp (10))";
        assertEquals(answer, result);
    }

    @Test
    void selectBinaryPlus() {
        ClickHouseConstant dConst = ClickHouseCreateConstant.createFloat32Constant((float) -1.1);
        ClickHouseConstant tenConst = ClickHouseCreateConstant.createInt32Constant(10);
        ClickHouseExpression expr = new ClickHouseBinaryArithmeticOperation(dConst, tenConst,
                ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator.ADD);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(expr));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT ((-1.1)+(10))";
        assertEquals(answer, result);
    }

    @Test
    void selectBinaryPow() {
        ClickHouseConstant threeConst = ClickHouseCreateConstant.createInt8Constant(3);
        ClickHouseConstant tenConst = ClickHouseCreateConstant.createInt32Constant(10);
        ClickHouseExpression expr = new ClickHouseBinaryFunctionOperation(threeConst, tenConst,
                ClickHouseBinaryFunctionOperation.ClickHouseBinaryFunctionOperator.POW);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(expr));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT pow(3,10)";
        assertEquals(answer, result);
    }

    @Test
    void selectBinaryLCM() {
        ClickHouseConstant aConst = ClickHouseCreateConstant.createInt8Constant(100);
        ClickHouseConstant bConst = ClickHouseCreateConstant.createInt32Constant(-100);
        ClickHouseExpression expr = new ClickHouseBinaryFunctionOperation(aConst, bConst,
                ClickHouseBinaryFunctionOperation.ClickHouseBinaryFunctionOperator.LCM);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(expr));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT lcm(100,-100)";
        assertEquals(answer, result);
    }

    @Test
    void selectBinaryDivCol() {
        ClickHouseColumnReference a = new ClickHouseColumnReference(new ClickHouseSchema.ClickHouseColumn("a",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null), null, null);
        ClickHouseColumnReference b = new ClickHouseColumnReference(new ClickHouseSchema.ClickHouseColumn("b",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null), null, null);
        ClickHouseExpression expr = new ClickHouseBinaryFunctionOperation(a, b,
                ClickHouseBinaryFunctionOperation.ClickHouseBinaryFunctionOperator.INT_DIV);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(expr));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT intDiv(a,b)";
        assertEquals(answer, result);
    }

    @Test
    void selectBinaryComp() {
        ClickHouseConstant aConst = ClickHouseCreateConstant.createInt8Constant(10);
        ClickHouseConstant bConst = ClickHouseCreateConstant.createInt32Constant(100);
        ClickHouseExpression expr = new ClickHouseBinaryComparisonOperation(aConst, bConst,
                ClickHouseBinaryComparisonOperation.ClickHouseBinaryComparisonOperator.GREATER);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(expr));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT ((10)>(100))";
        assertEquals(answer, result);
    }

}
