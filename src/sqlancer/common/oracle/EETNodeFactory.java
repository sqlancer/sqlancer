package sqlancer.common.oracle;

import sqlancer.common.ast.newast.Expression;

/**
 * Factory for constructing the AST nodes needed by the {@link EETTransformer EET transformer's} transformation rules.
 * Because every DBMS has its own expression AST, the actual node construction is DBMS-specific; this interface lets the
 * (DBMS-independent) transformation rules be expressed once in terms of a small set of primitive operations.
 *
 * @param <E>
 *            the DBMS-specific expression type
 */
public interface EETNodeFactory<E extends Expression<?>> {

    /** Builds {@code left AND right}. */
    E and(E left, E right);

    /** Builds {@code left OR right}. */
    E or(E left, E right);

    /** Builds {@code NOT expr}. */
    E not(E expr);

    /** Builds {@code expr IS NULL}. */
    E isNull(E expr);

    /** Builds {@code expr IS NOT NULL}. */
    E isNotNull(E expr);

    /** Builds {@code CASE WHEN condition THEN thenExpr ELSE elseExpr END}. */
    E caseWhen(E condition, E thenExpr, E elseExpr);

    /** Generates a fresh random boolean expression, reusing the variables available to the query generator. */
    E generateBooleanExpression();

    /**
     * Whether {@code expr} may be wrapped in a CASE WHEN expression. Some expressions (e.g. table references) are not
     * CASE-WHEN applicable and must be transformed to themselves (rule No. 7 of the EET paper).
     */
    boolean isCaseWhenApplicable(E expr);
}
