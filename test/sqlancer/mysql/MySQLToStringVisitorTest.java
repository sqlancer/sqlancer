package sqlancer.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLColumnReference;

public class MySQLToStringVisitorTest {
    
    @ParameterizedTest
    @EnumSource(MySQLAggregate.MySQLAggregateFunction.class)
    void visitAggregateWithOptions(MySQLAggregate.MySQLAggregateFunction function) {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a",
                MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference aRef = new MySQLColumnReference(aCol, null);

        MySQLToStringVisitor visitor = new MySQLToStringVisitor();

        for (String option : function.getOptions()) {
            visitor.visit(new MySQLAggregate(aRef, function, option));
            assertEquals(String.format("%s(%s a)", function, option), visitor.get());
        }
    }

    @ParameterizedTest
    @EnumSource(MySQLAggregate.MySQLAggregateFunction.class)
    void visitAggregateWithoutOptions(MySQLAggregate.MySQLAggregateFunction function) {
        MySQLSchema.MySQLColumn aCol = new MySQLSchema.MySQLColumn("a",
                MySQLSchema.MySQLDataType.INT, false, 0);
        MySQLColumnReference aRef = new MySQLColumnReference(aCol, null);

        MySQLToStringVisitor visitor = new MySQLToStringVisitor();

        visitor.visit(new MySQLAggregate(aRef, function, null));
        assertEquals(String.format("%s(a)", function), visitor.get());
    }
}
