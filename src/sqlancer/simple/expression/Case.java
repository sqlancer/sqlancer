package sqlancer.simple.expression;

import java.util.List;

import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class Case implements Expression {
    Expression switchCondition;
    List<Expression> conditions;
    List<Expression> expressions;
    Expression elseExpression;

    public Case(Generator gen) {
        this.switchCondition = gen.generateResponse(Signal.EXPRESSION);
        this.conditions = gen.generateResponse(Signal.EXPRESSION_LIST);
        this.expressions = gen.generateResponse(Signal.EXPRESSION_LIST);
        this.elseExpression = gen.generateResponse(Signal.EXPRESSION);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(conditions.size(), expressions.size()); i++) {
            sb.append(" WHEN ").append(conditions.get(i).print()).append(" THEN ").append(expressions.get(i).print());
        }

        return "CASE " + switchCondition.print() + " " + sb + " ELSE " + elseExpression.print() + " END";
    }
}
