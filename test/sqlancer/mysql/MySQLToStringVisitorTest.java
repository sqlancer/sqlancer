package sqlancer.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLCaseOperator;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLConstant.MySQLIntConstant;

public class MySQLToStringVisitorTest {

    @Test
    void visitAggregateToString() {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a", MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference aRef = new MySQLColumnReference(aCol, MySQLConstant.createNullConstant());

        MySQLAggregate aggrCount = new MySQLAggregate(List.of(aRef), MySQLAggregate.MySQLAggregateFunction.COUNT);
        assertEquals("COUNT(a)", MySQLVisitor.asString(aggrCount));

        MySQLAggregate aggrSum = new MySQLAggregate(List.of(aRef), MySQLAggregate.MySQLAggregateFunction.SUM);
        assertEquals("SUM(a)", MySQLVisitor.asString(aggrSum));

        MySQLAggregate aggrMin = new MySQLAggregate(List.of(aRef), MySQLAggregate.MySQLAggregateFunction.MIN);
        assertEquals("MIN(a)", MySQLVisitor.asString(aggrMin));

        MySQLAggregate aggrMax = new MySQLAggregate(List.of(aRef), MySQLAggregate.MySQLAggregateFunction.MAX);
        assertEquals("MAX(a)", MySQLVisitor.asString(aggrMax));
    }

    @Test
    void visitAggregateWithDistinctToString() {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a", MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference aRef = new MySQLColumnReference(aCol, MySQLConstant.createNullConstant());

        MySQLAggregate aggrCountDistinct = new MySQLAggregate(List.of(aRef),
                MySQLAggregate.MySQLAggregateFunction.COUNT_DISTINCT);
        assertEquals("COUNT(DISTINCT a)", MySQLVisitor.asString(aggrCountDistinct));

        MySQLAggregate aggrSumDistinct = new MySQLAggregate(List.of(aRef),
                MySQLAggregate.MySQLAggregateFunction.SUM_DISTINCT);
        assertEquals("SUM(DISTINCT a)", MySQLVisitor.asString(aggrSumDistinct));

        MySQLAggregate aggrMinDistinct = new MySQLAggregate(List.of(aRef),
                MySQLAggregate.MySQLAggregateFunction.MIN_DISTINCT);
        assertEquals("MIN(DISTINCT a)", MySQLVisitor.asString(aggrMinDistinct));

        MySQLAggregate aggrMaxDistinct = new MySQLAggregate(List.of(aRef),
                MySQLAggregate.MySQLAggregateFunction.MAX_DISTINCT);
        assertEquals("MAX(DISTINCT a)", MySQLVisitor.asString(aggrMaxDistinct));
    }

    @Test
    void visitCaseWhenToString() {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a", MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference switchExpr = new MySQLColumnReference(aCol, MySQLConstant.createNullConstant());

        List<MySQLExpression> whenExprs = List.of(MySQLIntConstant.createIntConstant(1),
                MySQLIntConstant.createIntConstant(2));

        List<MySQLExpression> thenExprs = List.of(MySQLIntConstant.createIntConstant(11),
                MySQLIntConstant.createIntConstant(22));

        MySQLConstant elseExpr = MySQLConstant.createIntConstant(0);

        assertEquals("(CASE a WHEN 1 THEN 11 WHEN 2 THEN 22 ELSE 0 END)",
                MySQLVisitor.asString(new MySQLCaseOperator(switchExpr, whenExprs, thenExprs, elseExpr)));
        assertEquals("(CASE WHEN 1 THEN 11 WHEN 2 THEN 22 ELSE 0 END)",
                MySQLVisitor.asString(new MySQLCaseOperator(null, whenExprs, thenExprs, elseExpr)));
        assertEquals("(CASE a WHEN 1 THEN 11 WHEN 2 THEN 22 END)",
                MySQLVisitor.asString(new MySQLCaseOperator(switchExpr, whenExprs, thenExprs, null)));
        assertEquals("(CASE WHEN 1 THEN 11 WHEN 2 THEN 22 END)",
                MySQLVisitor.asString(new MySQLCaseOperator(null, whenExprs, thenExprs, null)));
    }
}
