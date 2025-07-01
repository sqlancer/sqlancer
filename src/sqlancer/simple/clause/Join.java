package sqlancer.simple.clause;

import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public interface Join extends Clause {

    class Inner implements Join {
        Expression rightTable;
        Expression onCondition;

        public Inner(Generator gen) {
            this.rightTable = gen.generateResponse(Signal.TABLE_NAME);
            this.onCondition = gen.generateResponse(Signal.EXPRESSION);
        }

        @Override
        public String print() {
            if (rightTable == null) {
                return "";
            }
            return "INNER JOIN " + rightTable.print() + " ON " + onCondition.print();
        }
    }

    class Left implements Join {
        Expression rightTable;
        Expression onCondition;

        public Left(Generator gen) {
            this.rightTable = gen.generateResponse(Signal.TABLE_NAME);
            this.onCondition = gen.generateResponse(Signal.EXPRESSION);
        }

        @Override
        public String print() {
            if (rightTable == null) {
                return "";
            }
            return "LEFT JOIN " + rightTable.print() + " ON " + onCondition.print();
        }
    }

}
