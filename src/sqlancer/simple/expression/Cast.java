package sqlancer.simple.expression;

import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class Cast implements Expression {
    Expression inner;
    String typeName;

    public Cast(Expression inner, String typeName) {
        this.inner = inner;
        this.typeName = typeName;
    }

    public Cast(Generator gen) {
        this.inner = gen.generateResponse(Signal.EXPRESSION);
        this.typeName = gen.generateResponse(Signal.TYPE_NAME);
    }

    @Override
    public String print() {
        return "CAST ((" + inner.print() + ") AS " + typeName + ")";
    }
}
