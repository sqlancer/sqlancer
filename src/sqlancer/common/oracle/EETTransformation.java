package sqlancer.common.oracle;

import sqlancer.common.ast.newast.Expression;
import sqlancer.Randomly;

/**
 * Implements the semantic-preserving expression transformation rules of EET (Equivalent Expression Transformation, Jiang
 * &amp; Su, OSDI'24), Table 2. Given an expression, {@link #applyRandomRule} returns a semantically equivalent
 * expression built from the primitives provided by an {@link EETNodeFactory}. The rules are DBMS-independent; only the
 * node construction (via the factory) is DBMS-specific.
 *
 * <p>
 * The rules rely on two always-determined boolean expressions built from an arbitrary boolean {@code p}:
 * <ul>
 * <li>{@code true_expr(p)  = p OR (NOT p) OR (p IS NULL)}, which always evaluates to TRUE, and</li>
 * <li>{@code false_expr(p) = p AND (NOT p) AND (p IS NOT NULL)}, which always evaluates to FALSE.</li>
 * </ul>
 *
 * @param <E>
 *            the DBMS-specific expression type
 */
public class EETTransformation<E extends Expression<?>> {

    private final EETNodeFactory<E> factory;

    public EETTransformation(EETNodeFactory<E> factory) {
        this.factory = factory;
    }

    // true_expr(p) = p OR (NOT p) OR (p IS NULL) -> always TRUE
    private E trueExpr() {
        E p = factory.generateBooleanExpression();
        return factory.or(factory.or(p, factory.not(p)), factory.isNull(p));
    }

    // false_expr(p) = p AND (NOT p) AND (p IS NOT NULL) -> always FALSE
    private E falseExpr() {
        E p = factory.generateBooleanExpression();
        return factory.and(factory.and(p, factory.not(p)), factory.isNotNull(p));
    }

    /**
     * Transforms {@code expr} into a semantically equivalent expression by applying a randomly chosen applicable
     * transformation rule.
     *
     * @param expr
     *            the expression to transform
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value (e.g. a WHERE predicate or an operand of a
     *            logical operator). Only in a boolean context may the determined-boolean rules (No. 1 and 2), which
     *            reduce the expression to a boolean value, be applied; in a scalar context they would change the
     *            expression's value/type and are therefore excluded.
     *
     * @return a semantically equivalent expression
     */
    public E applyRandomRule(E expr, boolean booleanContext) {
        int rule;
        if (booleanContext) {
            // Rules No. 1-6 are all value-preserving in a boolean context.
            rule = Randomly.fromOptions(1, 2, 3, 4, 5, 6);
        } else {
            if (!factory.isCaseWhenApplicable(expr)) {
                return expr; // rule No. 7: transform the expression to itself
            }
            // In a scalar context only the CASE WHEN rules preserve the exact value and type.
            rule = Randomly.fromOptions(3, 4, 5, 6);
        }
        switch (rule) {
        case 1: // bool_expr => false_expr OR bool_expr
            return factory.or(falseExpr(), expr);
        case 2: // bool_expr => true_expr AND bool_expr
            return factory.and(trueExpr(), expr);
        case 3: // expr => CASE WHEN false_expr THEN copy(expr) ELSE expr END
            return factory.caseWhen(falseExpr(), expr, expr);
        case 4: // expr => CASE WHEN true_expr THEN expr ELSE copy(expr) END
            return factory.caseWhen(trueExpr(), expr, expr);
        case 5: // expr => CASE WHEN rand_bool THEN copy(expr) ELSE expr END
        case 6: // expr => CASE WHEN rand_bool THEN expr ELSE copy(expr) END
            return factory.caseWhen(factory.generateBooleanExpression(), expr, expr);
        default:
            throw new AssertionError(rule);
        }
    }
}
