package sqlancer.qpg.materialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializePrefixOperation;
import sqlancer.materialize.ast.MaterializePrefixOperation.PrefixOperator;

public class TestMaterializeUnaryPlus {

    @Test
    public void testUnaryPlusConvertToDouble() {
        // Test UNARY_PLUS on int
        MaterializeConstant intConstant = MaterializeConstant.createIntConstant(42);
        MaterializeExpression unaryPlusInt = new MaterializePrefixOperation(intConstant, PrefixOperator.UNARY_PLUS);
        MaterializeConstant result = unaryPlusInt.getExpectedValue();
        
        // Verify result type is FLOAT (double precision)
        assertEquals(MaterializeDataType.FLOAT, unaryPlusInt.getExpressionType());
        
        // Verify value is converted to double
        assertTrue(result.isFloat());
        assertEquals(42.0, result.asDouble());
        
        // Test UNARY_PLUS on boolean
        MaterializeConstant boolConstant = MaterializeConstant.createBooleanConstant(true);
        MaterializeExpression unaryPlusBool = new MaterializePrefixOperation(boolConstant, PrefixOperator.UNARY_PLUS);
        result = unaryPlusBool.getExpectedValue();
        
        // Verify result type is FLOAT
        assertEquals(MaterializeDataType.FLOAT, unaryPlusBool.getExpressionType());
        
        // Verify value is converted to double (true becomes 1.0)
        assertTrue(result.isFloat());
        assertEquals(1.0, result.asDouble());
        
        // Test UNARY_PLUS on string
        MaterializeConstant stringConstant = MaterializeConstant.createTextConstant("123.45");
        MaterializeExpression unaryPlusString = new MaterializePrefixOperation(stringConstant, PrefixOperator.UNARY_PLUS);
        result = unaryPlusString.getExpectedValue();
        
        // Verify result type is FLOAT
        assertEquals(MaterializeDataType.FLOAT, unaryPlusString.getExpressionType());
        
        // Verify value is converted to double
        assertTrue(result.isFloat());
        assertEquals(123.45, result.asDouble());
    }
}