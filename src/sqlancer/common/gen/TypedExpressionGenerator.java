package sqlancer.common.gen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;

public abstract class TypedExpressionGenerator<E, C, T> implements ExpressionGenerator<E> {

    protected List<C> columns = Collections.emptyList();
    protected boolean allowAggregates;

    public E generateExpression(T type) {
        return generateExpression(type, 0);
    }

    public abstract E generateConstant(T type);

    protected abstract E generateExpression(T type, int depth);

    protected abstract E generateColumn(T type);

    protected abstract T getRandomType();

    protected abstract boolean canGenerateColumnOfType(T type);

    @SuppressWarnings("unchecked") // unsafe
    public <U extends TypedExpressionGenerator<E, C, T>> U setColumns(List<C> columns) {
        this.columns = columns;
        return (U) this;
    }

    public E generateLeafNode(T type) {
        if (Randomly.getBooleanWithRatherLowProbability() || !canGenerateColumnOfType(type)) {
            return generateConstant(type);
        } else {
            return generateColumn(type);
        }
    }

    public List<E> generateExpressions(T type, int nr) {
        List<E> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(type));
        }
        return expressions;
    }

    public List<E> generateExpressions(T type, int nr, int depth) {
        List<E> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(type, depth));
        }
        return expressions;
    }

    public List<E> generateExpressions(int nr) {
        List<E> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(getRandomType()));
        }
        return expressions;
    }

    // override this class to also generate ASC, DESC
    public List<E> generateOrderBys() {
        return generateExpressions(Randomly.smallNumber() + 1);
    }

    // override this class to generate aggregate functions
    // public E generateHavingClause() {
    // allowAggregates = true;
    // E expr = generateExpression();
    // allowAggregates = false;
    // return expr;
    // }

}
