package sqlancer.simple.gen;

import sqlancer.simple.expression.Expression;
import sqlancer.simple.statement.Select;

public interface TLPGenerator {
    Select generateSelect();

    Expression generateExpression();
}
