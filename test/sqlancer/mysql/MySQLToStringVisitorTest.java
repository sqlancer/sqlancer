package sqlancer.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLColumnReference;

public class MySQLToStringVisitorTest {

    @Test
    void visitAggregateToString() {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a", MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference aRef = new MySQLColumnReference(aCol, null);

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
        MySQLColumnReference aRef = new MySQLColumnReference(aCol, null);

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
    public void testMySQLNoRECOracle() {
        assertEquals(0, Main.executeMain("--num-threads", "4",
                "--num-tries", "100", "--num-queries", "5000", "--max-generated-databases", "1",
                "--host", "localhost", "--port", "3306", "--username", "springstudent", "--password", "springstudent",
                "mysql", "--oracle", "TLP_WHERE"));
    }
}
