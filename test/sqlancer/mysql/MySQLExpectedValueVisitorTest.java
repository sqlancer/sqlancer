package sqlancer.mysql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.mysql.ast.MySQLCaseOperator;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLConstant.MySQLIntConstant;
import sqlancer.mysql.ast.MySQLExpression;

public class MySQLExpectedValueVisitorTest {

    @Test
    void testCaseOp() {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a", MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference switchExpr = new MySQLColumnReference(aCol, null);
        List<MySQLExpression> whenExprs = List.of(MySQLIntConstant.createIntConstant(1));
        List<MySQLExpression> thenExprs = List.of(MySQLIntConstant.createIntConstant(11));
        MySQLConstant elseExpr = MySQLConstant.createIntConstant(0);

        MySQLExpectedValueVisitor visitor = new MySQLExpectedValueVisitor();

        AssertionError err = assertThrows(AssertionError.class,
                () -> visitor.visit(new MySQLCaseOperator(switchExpr, whenExprs, thenExprs, elseExpr)));

        assertTrue(err.getMessage().contains("PQS not supported"));
    }
}
