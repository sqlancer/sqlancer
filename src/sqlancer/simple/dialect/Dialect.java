package sqlancer.simple.dialect;

import java.util.List;

import sqlancer.simple.clause.Clause;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.statement.Select;
import sqlancer.simple.type.Type;

public interface Dialect {
    Expression map(Expression expression);

    Clause map(Clause expression);

    List<Type> getTypes();

    List<Class<? extends Expression>> getLegalExpressions();

    Select generateSelect(Generator gen);
}
