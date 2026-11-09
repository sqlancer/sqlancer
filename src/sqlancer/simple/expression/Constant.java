package sqlancer.simple.expression;

import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class Constant implements Expression {
    String value;

    public Constant(String value) {
        this.value = value;
    }

    public Constant(Generator gen) {
        Constant constant = gen.generateResponse(Signal.CONSTANT_VALUE);
        this.value = constant.value;
    }

    @Override
    public String print() {
        return value;
    }
}
