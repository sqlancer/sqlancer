package sqlancer.common.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;

/**
 * Abstract base class for EET (Equivalent Expression Transformation) tree-walkers, based on "Detecting Logic Bugs in
 * Database Engines via Equivalent Expression Transformation" (Jiang &amp; Su, OSDI'24).
 *
 * <p>
 * This class implements the seven transformation rules (Table 2 of the paper) and provides a template-method framework
 * for applying them throughout an expression's AST. Subclasses implement {@link #descend} to rebuild DBMS-specific AST
 * nodes from their transformed children, the abstract factory methods to construct new nodes, and the type hooks
 * ({@link #inferType} and {@link #generateExpressionOfType}) that realize the paper's {@code rand_expr(type(expr))};
 * everything else (the rule logic, context threading, and tree-walking orchestration) is provided here.
 *
 * @param <E>
 *            the DBMS-specific expression class
 * @param <T>
 *            the DBMS-specific type domain used by {@link #inferType} and {@link #generateExpressionOfType}
 */
public abstract class EETTransformer<E extends Expression<?>, T> {

    // true_expr(p) = p OR (NOT p) OR (p IS NULL) -> always TRUE
    private E trueExpr() {
        E p = generateBooleanExpression();
        return orExpr(orExpr(p, not(p)), isNull(p));
    }

    // false_expr(p) = p AND (NOT p) AND (p IS NOT NULL) -> always FALSE
    private E falseExpr() {
        E p = generateBooleanExpression();
        return and(and(p, not(p)), isNotNull(p));
    }

    /**
     * Implements the paper's {@code rand_expr(type(expr))}: a random expression whose static type matches that of
     * {@code expr}. Although the generated expression is never evaluated (it occupies the redundant branch of rules 3
     * and 4), its static type participates in the DBMS's CASE WHEN result-type resolution, so a type mismatch could
     * alter the live branch's value or rendering. When the type of {@code expr} cannot be inferred, this falls back to
     * {@code expr} itself, which trivially has the correct type (degenerating to rules 5 and 6).
     *
     * @param expr
     *            the expression whose static type the generated expression must match
     *
     * @return a random expression whose static type matches that of {@code expr}
     */
    private E randExprOfSameType(E expr) {
        T type = inferType(expr);
        if (type == null) {
            return expr;
        }
        return generateExpressionOfType(type);
    }

    /**
     * The first six transformation rules of the EET paper (Table 2). Each rule knows how to apply itself
     * ({@link #apply}) and in which contexts it preserves the expression's value ({@link #isApplicable}). Rule No. 7
     * (transform the expression to itself) is not modelled here: it is the fallback applied by {@link #applyRandomRule}
     * when no other rule is applicable.
     */
    private enum Rule {
        // expr => false_expr OR expr
        RULE_1 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, E expr) {
                return t.orExpr(t.falseExpr(), expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                // Reduces the expression to a boolean value, so it is value-preserving only in a boolean context.
                return booleanContext;
            }
        },
        // expr => true_expr AND expr
        RULE_2 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, E expr) {
                return t.and(t.trueExpr(), expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                // Reduces the expression to a boolean value, so it is value-preserving only in a boolean context.
                return booleanContext;
            }
        },
        // expr => CASE WHEN false_expr THEN rand_expr(type(expr)) ELSE expr END
        RULE_3 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, E expr) {
                return t.caseWhen(t.falseExpr(), t.randExprOfSameType(expr), expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }
        },
        // expr => CASE WHEN true_expr THEN expr ELSE rand_expr(type(expr)) END
        RULE_4 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, E expr) {
                return t.caseWhen(t.trueExpr(), expr, t.randExprOfSameType(expr));
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }
        },
        // expr => CASE WHEN rand_expr(boolean) THEN copy(expr) ELSE expr END
        RULE_5 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, E expr) {
                // deep copy of expr is not needed, as the AST nodes are immutable anyway
                return t.caseWhen(t.generateBooleanExpression(), expr, expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }
        },
        // expr => CASE WHEN rand_expr(boolean) THEN expr ELSE copy(expr) END
        RULE_6 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, E expr) {
                // deep copy of expr is not needed, as the AST nodes are immutable anyway
                return t.caseWhen(t.generateBooleanExpression(), expr, expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }
        };

        /**
         * Applies this rule to {@code expr}, producing a semantically equivalent expression.
         *
         * @param <E>
         *            the DBMS-specific expression class
         * @param <T>
         *            the DBMS-specific type domain
         * @param t
         *            the transformer providing the DBMS-specific node factories
         * @param expr
         *            the expression to transform
         *
         * @return a semantically equivalent expression
         */
        abstract <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, E expr);

        /**
         * Whether this rule preserves {@code expr}'s value in the given context.
         *
         * @param booleanContext
         *            whether {@code expr} is evaluated purely for its truth value (rules 1 and 2 are only applicable if
         *            this is the case)
         * @param caseWhenApplicable
         *            whether {@code expr} may be wrapped in a CASE WHEN expression
         *
         * @return {@code true} if this rule preserves {@code expr}'s value in the given context
         */
        abstract boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable);
    }

    /**
     * Applies a randomly chosen applicable transformation rule to {@code expr}, returning a semantically equivalent
     * expression. When no rule is applicable, {@code expr} is returned unchanged (rule No. 7 of the EET paper).
     *
     * @param expr
     *            the expression to transform
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value
     *
     * @return a semantically equivalent expression
     */
    protected E applyRandomRule(E expr, boolean booleanContext) {
        boolean caseWhenApplicable = isCaseWhenApplicable(expr);
        List<Rule> applicableRules = new ArrayList<>();
        for (Rule rule : Rule.values()) {
            if (rule.isApplicable(booleanContext, caseWhenApplicable)) {
                applicableRules.add(rule);
            }
        }
        if (applicableRules.isEmpty()) {
            return expr; // rule 7 fallback: transform expression to itself
        }
        return Randomly.fromList(applicableRules).apply(this, expr);
    }

    /**
     * Transforms {@code expr} into a semantically equivalent expression. A transformation rule is always applied at the
     * root, guaranteeing (unless only rule 7 is applicable) that the returned expression differs from the input.
     *
     * @param expr
     *            the expression to transform
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value
     *
     * @return a semantically equivalent expression
     */
    public E transform(E expr, boolean booleanContext) {
        return transformNode(expr, booleanContext, true);
    }

    /**
     * Descends into {@code expr}, rebuilds it from transformed children, then optionally applies a rule at this node.
     *
     * @param expr
     *            the expression to transform
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value
     * @param forceApply
     *            whether a rule must be applied at this node rather than only with some probability
     *
     * @return the transformed expression
     */
    protected E transformNode(E expr, boolean booleanContext, boolean forceApply) {
        E descended = descend(expr, booleanContext);
        if (forceApply || Randomly.getBoolean()) {
            return applyRandomRule(descended, booleanContext);
        }
        return descended;
    }

    /**
     * Rebuilds {@code expr} with its children transformed, threading the correct boolean/scalar context into each
     * child. Leaf nodes (columns, constants, table references, ...) should be returned unchanged; any applicable
     * transformation will still be applied to them by the calling {@link #transformNode}.
     *
     * @param expr
     *            the expression to descend into
     * @param booleanContext
     *            the context in which {@code expr} itself is evaluated (used to determine child contexts)
     *
     * @return a rebuilt copy of {@code expr} with transformed children, or {@code expr} itself if it is a leaf
     */
    protected abstract E descend(E expr, boolean booleanContext);

    /**
     * Builds {@code left AND right}.
     *
     * @param left
     *            the left operand
     * @param right
     *            the right operand
     *
     * @return the {@code left AND right} expression
     */
    protected abstract E and(E left, E right);

    /**
     * Builds {@code left OR right}.
     *
     * @param left
     *            the left operand
     * @param right
     *            the right operand
     *
     * @return the {@code left OR right} expression
     */
    protected abstract E orExpr(E left, E right);

    /**
     * Builds {@code NOT expr}.
     *
     * @param expr
     *            the operand
     *
     * @return the {@code NOT expr} expression
     */
    protected abstract E not(E expr);

    /**
     * Builds {@code expr IS NULL}.
     *
     * @param expr
     *            the operand
     *
     * @return the {@code expr IS NULL} expression
     */
    protected abstract E isNull(E expr);

    /**
     * Builds {@code expr IS NOT NULL}.
     *
     * @param expr
     *            the operand
     *
     * @return the {@code expr IS NOT NULL} expression
     */
    protected abstract E isNotNull(E expr);

    /**
     * Builds {@code CASE WHEN condition THEN thenExpr ELSE elseExpr END}.
     *
     * @param condition
     *            the WHEN condition
     * @param thenExpr
     *            the THEN branch
     * @param elseExpr
     *            the ELSE branch
     *
     * @return the CASE WHEN expression
     */
    protected abstract E caseWhen(E condition, E thenExpr, E elseExpr);

    /**
     * Generates a fresh random boolean expression, reusing the variables available to the query generator.
     *
     * @return a fresh random boolean expression
     */
    protected abstract E generateBooleanExpression();

    /**
     * Infers the static type of {@code expr}, or returns {@code null} if it cannot be determined. The type domain
     * {@code T} is DBMS-specific and may be coarse: it only needs to be precise enough that replacing an expression
     * with another of the same {@code T} leaves the DBMS's CASE WHEN result-type resolution unaffected. Returning
     * {@code null} is always safe — rules No. 3 and 4 then fall back to reusing {@code expr} itself as the dead branch.
     * Inference should therefore be conservative: prefer {@code null} over a type whose CASE WHEN behaviour is
     * uncertain.
     *
     * @param expr
     *            the expression whose static type is to be inferred
     *
     * @return the inferred static type of {@code expr}, or {@code null} if it cannot be determined
     */
    protected abstract T inferType(E expr);

    /**
     * Generates a fresh random expression of static type {@code type}, reusing the variables available to the query
     * generator. DBMSs with a typed expression generator can delegate to it directly; DBMSs with an untyped generator
     * can instead wrap an arbitrary random expression in a CAST to {@code type} (which requires every value of
     * {@code T} to be a valid CAST target).
     *
     * @param type
     *            the static type the generated expression must have
     *
     * @return a fresh random expression of static type {@code type}
     */
    protected abstract E generateExpressionOfType(T type);

    /**
     * Whether {@code expr} may be wrapped in a CASE WHEN expression. Some expressions (e.g. table references) are not
     * CASE-WHEN applicable and must be transformed to themselves (rule No. 7 of the EET paper).
     *
     * @param expr
     *            the expression to test
     *
     * @return {@code true} if {@code expr} may be wrapped in a CASE WHEN expression
     */
    protected abstract boolean isCaseWhenApplicable(E expr);
}
