package sqlancer.mysql.ast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.ast.MySQLConstant.MySQLIntConstant;

public class MySQLCaseOperatorTest {

    @Test
    void getExpectedValue_switchConditionMatchesWhen_ReturnsThen() {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a", MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference switchExpr = new MySQLColumnReference(aCol, MySQLIntConstant.createIntConstant(1));
        List<MySQLExpression> whenExprs = List.of(MySQLIntConstant.createIntConstant(1),
                MySQLIntConstant.createIntConstant(2));
        List<MySQLExpression> thenExprs = List.of(MySQLIntConstant.createIntConstant(11),
                MySQLIntConstant.createIntConstant(22));
        MySQLConstant elseExpr = MySQLConstant.createIntConstant(0);

        MySQLCaseOperator caseOperator = new MySQLCaseOperator(switchExpr, whenExprs, thenExprs, elseExpr);

        assertEquals(11, caseOperator.getExpectedValue().getInt());
    }

    @Test
    void getExpectedValue_switchConditionHasNoMatches_ReturnsElse() {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a", MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference switchExpr = new MySQLColumnReference(aCol, MySQLIntConstant.createNullConstant());
        List<MySQLExpression> whenExprs = List.of(MySQLIntConstant.createIntConstant(1),
                MySQLIntConstant.createIntConstant(2));
        List<MySQLExpression> thenExprs = List.of(MySQLIntConstant.createIntConstant(11),
                MySQLIntConstant.createIntConstant(22));
        MySQLConstant elseExpr = MySQLConstant.createIntConstant(0);

        assertEquals(0, new MySQLCaseOperator(switchExpr, whenExprs, thenExprs, elseExpr).getExpectedValue().getInt());
        assertTrue(new MySQLCaseOperator(switchExpr, whenExprs, thenExprs, null).getExpectedValue().isNull());
    }

    @Test
    void getExpectedValue_whenTrue_ReturnsThen() {
        List<MySQLExpression> whenExprs = List.of(MySQLIntConstant.createIntConstant(1),
                MySQLIntConstant.createIntConstant(2));
        List<MySQLExpression> thenExprs = List.of(MySQLIntConstant.createIntConstant(11),
                MySQLIntConstant.createIntConstant(22));
        MySQLConstant elseExpr = MySQLConstant.createIntConstant(0);
        MySQLCaseOperator caseOperator = new MySQLCaseOperator(null, whenExprs, thenExprs, elseExpr);

        assertEquals(11, caseOperator.getExpectedValue().getInt());
    }

    @Test
    void getExpectedValue_whenAllFalse_ReturnsElse() {
        List<MySQLExpression> whenExprs = List.of(MySQLIntConstant.createBoolean(false),
                MySQLIntConstant.createBoolean(false));
        List<MySQLExpression> thenExprs = List.of(MySQLIntConstant.createIntConstant(11),
                MySQLIntConstant.createIntConstant(22));
        MySQLConstant elseExpr = MySQLConstant.createIntConstant(0);

        assertEquals(0, new MySQLCaseOperator(null, whenExprs, thenExprs, elseExpr).getExpectedValue().getInt());
        assertTrue(new MySQLCaseOperator(null, whenExprs, thenExprs, null).getExpectedValue().isNull());
    }
}
