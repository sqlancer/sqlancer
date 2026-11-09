package sqlancer.simple.clause;

import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public interface Where extends Clause {

    class Of implements Where {
        Expression condition;

        public Of(Expression condition) {
            this.condition = condition;
        }

        public Of(Generator gen) {
            this.condition = gen.generateResponse(Signal.EXPRESSION);
        }

        @Override
        public String print() {
            return "WHERE " + condition.print();
        }
    }

    class Empty implements Where {

        public Empty() {
        }

        @SuppressWarnings("unused")
        public Empty(Generator gen) {
        }

        @Override
        public String print() {
            return "";
        }

    }
}
