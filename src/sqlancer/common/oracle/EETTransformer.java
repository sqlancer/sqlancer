package sqlancer.common.oracle;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;

/**
 * Abstract base class for EET (Equivalent Expression Transformation) tree-walkers, based on "Detecting Logic Bugs in
 * Database Engines via Equivalent Expression Transformation" (Jiang &amp; Su, OSDI'24).
 *
 * <p>
 * This class implements the seven transformation rules (Table 2 of the paper) and provides a template-method framework
 * for applying them throughout an expression's AST. Subclasses implement {@link #descend} to rebuild DBMS-specific AST
 * nodes from their transformed children; everything else (the rule logic, context threading, and tree-walking
 * orchestration) is provided here.
 *
 * @param <E>
 *            the DBMS-specific expression type
 */
public abstract class EETTransformer<E extends Expression<?>> {

    private final EETNodeFactory<E> factory;

    protected EETTransformer(EETNodeFactory<E> factory) {
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
     * Applies a randomly chosen applicable transformation rule to {@code expr}, returning a semantically equivalent
     * expression.
     *
     * @param expr
     *            the expression to transform
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value; only in a boolean context may the
     *            determined-boolean rules (No. 1 and 2), which reduce the expression to a boolean value, be applied
     *
     * @return a semantically equivalent expression
     */
    protected E applyRandomRule(E expr, boolean booleanContext) {
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

    /**
     * Transforms {@code expr} into a semantically equivalent expression. A transformation rule is always applied at the
     * root, guaranteeing (unless only rule 7 is applicable) that the returned expression differs from the input.
     */
    public E transform(E expr, boolean booleanContext) {
        return transformNode(expr, booleanContext, true);
    }

    /**
     * Descends into {@code expr}, rebuilds it from transformed children, then optionally applies a rule at this node.
     */
    protected E transformNode(E expr, boolean booleanContext, boolean forceApply) {
        E descended = descend(expr, booleanContext);
        if (forceApply || Randomly.getBoolean()) {
            return applyRandomRule(descended, booleanContext);
        }
        return descended;
    }

    /**
     * Rebuilds {@code expr} with its children transformed, threading the correct boolean/scalar context into each child.
     * Leaf nodes (columns, constants, table references, ...) should be returned unchanged; any applicable transformation
     * will still be applied to them by the calling {@link #transformNode}.
     *
     * @param expr
     *            the expression to descend into
     * @param booleanContext
     *            the context in which {@code expr} itself is evaluated (used to determine child contexts)
     *
     * @return a rebuilt copy of {@code expr} with transformed children, or {@code expr} itself if it is a leaf
     */
    protected abstract E descend(E expr, boolean booleanContext);
}
