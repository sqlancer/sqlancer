package sqlancer.simple.clause;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public interface Fetch extends Clause {

    class All implements Fetch {

        @SuppressWarnings("unused")
        public All(Generator gen) {

        }

        @Override
        public String print() {
            return "*";
        }
    }

    class Column implements Fetch {
        List<Expression> columns;

        public Column(Generator gen) {
            this.columns = gen.generateResponse(Signal.COLUMN_NAME_LIST);
        }

        @Override
        public String print() {
            return columns.stream().map(Expression::print).collect(Collectors.joining(", "));
        }
    }

    class CountAll implements Clause {

        @SuppressWarnings("unused")
        public CountAll(Generator gen) {

        }

        @Override
        public String print() {
            return "COUNT(*)";
        }
    }
}
