package sqlancer.simple.clause;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class From implements Clause {
    List<Expression> tableNames;

    public From(Generator gen) {
        this.tableNames = gen.generateResponse(Signal.TABLE_NAME_LIST);
    }

    @Override
    public String print() {
        return tableNames.stream().map(Expression::print).collect(Collectors.joining(", "));
    }
}
