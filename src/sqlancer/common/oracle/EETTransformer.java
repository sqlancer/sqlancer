package sqlancer.common.oracle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
 * <p>
 * Every {@link #transform} call records the rule applications it performs. The resulting {@link TransformationRecord}
 * can later be passed to {@link #replay}, which re-applies the recorded rules with any subset of them disabled; because
 * each rule application is individually equivalence-preserving, every such replay yields an expression that is still
 * semantically equivalent to the input. Test-case reduction uses this to undo transformations one subset at a time
 * while preserving the oracle's soundness.
 *
 * @param <E>
 *            the DBMS-specific expression class
 * @param <T>
 *            the DBMS-specific type domain used by {@link #inferType} and {@link #generateExpressionOfType}
 */
public abstract class EETTransformer<E extends Expression<?>, T> {

    private List<Application<E, T>> recording; // non-null while transform() is recording its rule applications
    private TransformationRecord lastRecord;

    private List<Application<E, T>> replayApplications; // non-null while replay() is re-applying a record
    private SiteDirectives replayDirectives;
    private int replayCursor;
    private int replaySiteCursor;

    /**
     * Per-site directives consulted during {@link #replay}, keyed by the record-local site index. Every combination of
     * directives yields an expression that is semantically equivalent to the replayed one: a disabled site's rule is
     * not applied at all; a constant condition renders a site's always-true (or always-false) condition as the literal
     * constant of the same truth value; a copied dead branch replaces a site's generated dead-branch expression with a
     * copy of the live expression (the {@code copy_expr} form of rules No. 5 and 6).
     */
    public interface SiteDirectives {

        /**
         * Whether the site's rule is applied at all.
         *
         * @param site
         *            the record-local site index
         *
         * @return {@code true} if the site's rule is applied
         */
        boolean isEnabled(int site);

        /**
         * Whether the site's condition is rendered as a literal constant instead of the recorded
         * {@code true_expr}/{@code false_expr} scaffolding (or, for rules No. 5 and 6, the recorded condition).
         *
         * @param site
         *            the record-local site index
         *
         * @return {@code true} if the site's condition is rendered as a literal constant
         */
        boolean useConstantCondition(int site);

        /**
         * Whether the site's generated dead branch (rules No. 3 and 4 only) is replaced by a copy of the live
         * expression.
         *
         * @param site
         *            the record-local site index
         *
         * @return {@code true} if the site's dead branch is replaced by a copy of the live expression
         */
        boolean useCopiedDeadBranch(int site);
    }

    /**
     * The decision made at one {@link #transformNode} call: which rule (if any) was applied at that node, together with
     * the auxiliary expressions the rule drew randomly. Recording these decisions makes a transformation replayable
     * with any subset of its rule applications disabled (see {@link #replay}).
     *
     * @param <E>
     *            the DBMS-specific expression class
     * @param <T>
     *            the DBMS-specific type domain
     */
    private static final class Application<E, T> {
        private static final Application<?, ?> NONE = new Application<>(null, null, null, null);

        private final Rule rule; // null when no rule was applied at this node
        private final E auxiliary; // rules No. 1-4: the fresh predicate p; rules No. 5 and 6: the CASE WHEN condition
        private final E deadBranch; // rules No. 3 and 4: the recorded rand_expr, or null when it degenerated to a copy
        private final T deadBranchType; // rules No. 3 and 4: the inferred type deadBranch was generated for

        Application(Rule rule, E auxiliary, E deadBranch, T deadBranchType) {
            this.rule = rule;
            this.auxiliary = auxiliary;
            this.deadBranch = deadBranch;
            this.deadBranchType = deadBranchType;
        }
    }

    /**
     * The rule applications recorded by one {@link #transform} call. The record is opaque: callers can only query the
     * number of transformation sites and pass the record back to {@link #replay} on the transformer that produced it.
     */
    public static final class TransformationRecord {
        private final List<Application<?, ?>> applications;
        private final int siteCount;

        private TransformationRecord(List<Application<?, ?>> applications) {
            this.applications = applications;
            this.siteCount = (int) applications.stream().filter(application -> application.rule != null).count();
        }

        /**
         * The number of transformation sites (rule applications) in this record. {@link #replay} numbers the sites
         * {@code 0} to {@code getSiteCount() - 1} in the order they were recorded.
         *
         * @return the number of transformation sites
         */
        public int getSiteCount() {
            return siteCount;
        }

        /**
         * The sites whose rule application embeds a generated dead-branch expression (rules No. 3 and 4 with an
         * inferrable type), which a {@link #replay} may replace with a copy of the live expression.
         *
         * @return the record-local indices of the sites with a generated dead branch
         */
        public Set<Integer> getDeadBranchSites() {
            Set<Integer> deadBranchSites = new HashSet<>();
            int site = 0;
            for (Application<?, ?> application : applications) {
                if (application.rule != null) {
                    if (application.deadBranch != null) {
                        deadBranchSites.add(site);
                    }
                    site++;
                }
            }
            return deadBranchSites;
        }
    }

    // true_expr(p) = p OR (NOT p) OR (p IS NULL) -> always TRUE, for any predicate p
    private E trueExpr(E p) {
        return orExpr(orExpr(p, not(p)), isNull(p));
    }

    // false_expr(p) = p AND (NOT p) AND (p IS NOT NULL) -> always FALSE, for any predicate p
    private E falseExpr(E p) {
        return and(and(p, not(p)), isNotNull(p));
    }

    /**
     * The first six transformation rules of the EET paper (Table 2). Each rule knows how to apply itself
     * ({@link #apply}) and in which contexts it preserves the expression's value ({@link #isApplicable}). Rule No. 7
     * (transform the expression to itself) is not modelled here: it is the fallback applied by {@link #applyRandomRule}
     * when no other rule is applicable.
     *
     * <p>
     * A rule draws no randomness of its own: the auxiliary expressions it wraps the transformed expression in come from
     * the {@link Application} recorded for it, so applying a rule again during {@link EETTransformer#replay} reproduces
     * the same expression.
     */
    private enum Rule {
        // expr => false_expr OR expr
        RULE_1 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, Application<E, T> application, E expr,
                    boolean constantCondition, boolean copiedDeadBranch) {
                return t.orExpr(constantCondition ? t.falseConstant() : t.falseExpr(application.auxiliary), expr);
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
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, Application<E, T> application, E expr,
                    boolean constantCondition, boolean copiedDeadBranch) {
                return t.and(constantCondition ? t.trueConstant() : t.trueExpr(application.auxiliary), expr);
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
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, Application<E, T> application, E expr,
                    boolean constantCondition, boolean copiedDeadBranch) {
                return t.caseWhen(constantCondition ? t.falseConstant() : t.falseExpr(application.auxiliary),
                        t.deadBranch(application, expr, copiedDeadBranch), expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }

            @Override
            boolean usesDeadBranch() {
                return true;
            }
        },
        // expr => CASE WHEN true_expr THEN expr ELSE rand_expr(type(expr)) END
        RULE_4 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, Application<E, T> application, E expr,
                    boolean constantCondition, boolean copiedDeadBranch) {
                return t.caseWhen(constantCondition ? t.trueConstant() : t.trueExpr(application.auxiliary), expr,
                        t.deadBranch(application, expr, copiedDeadBranch));
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }

            @Override
            boolean usesDeadBranch() {
                return true;
            }
        },
        // expr => CASE WHEN rand_expr(boolean) THEN copy(expr) ELSE expr END
        RULE_5 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, Application<E, T> application, E expr,
                    boolean constantCondition, boolean copiedDeadBranch) {
                // deep copy of expr is not needed, as the AST nodes are immutable anyway
                return t.caseWhen(constantCondition ? t.trueConstant() : application.auxiliary, expr, expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }
        },
        // expr => CASE WHEN rand_expr(boolean) THEN expr ELSE copy(expr) END
        RULE_6 {
            @Override
            <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, Application<E, T> application, E expr,
                    boolean constantCondition, boolean copiedDeadBranch) {
                // deep copy of expr is not needed, as the AST nodes are immutable anyway
                return t.caseWhen(constantCondition ? t.trueConstant() : application.auxiliary, expr, expr);
            }

            @Override
            boolean isApplicable(boolean booleanContext, boolean caseWhenApplicable) {
                return caseWhenApplicable;
            }
        };

        /**
         * Applies this rule to {@code expr}, producing a semantically equivalent expression built from the auxiliary
         * expressions {@code application} recorded for it. With {@code constantCondition}, the rule's condition is
         * rendered as the literal constant of its (fixed) truth value: {@code true_expr}/{@code false_expr} are
         * TRUE/FALSE for every embedded predicate, and the condition of rules No. 5 and 6 is irrelevant since both
         * branches are identical, so this always preserves equivalence.
         *
         * @param <E>
         *            the DBMS-specific expression class
         * @param <T>
         *            the DBMS-specific type domain
         * @param t
         *            the transformer providing the DBMS-specific node factories
         * @param application
         *            the recorded application of this rule, supplying its auxiliary expressions
         * @param expr
         *            the expression to transform
         * @param constantCondition
         *            whether the rule's condition is rendered as a literal constant
         * @param copiedDeadBranch
         *            whether the rule's generated dead branch (rules No. 3 and 4) is replaced by a copy of {@code expr}
         *
         * @return a semantically equivalent expression
         */
        abstract <E extends Expression<?>, T> E apply(EETTransformer<E, T> t, Application<E, T> application, E expr,
                boolean constantCondition, boolean copiedDeadBranch);

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

        /**
         * Whether an application of this rule carries a dead branch: an expression occupying the redundant branch of
         * its CASE WHEN, which is never evaluated (see {@link EETTransformer#randomApplication}).
         *
         * @return {@code true} if applications of this rule carry a dead branch
         */
        boolean usesDeadBranch() {
            return false;
        }
    }

    /**
     * Applies a randomly chosen applicable transformation rule to {@code expr}, returning a semantically equivalent
     * expression. When no rule is applicable, {@code expr} is returned unchanged (rule No. 7 of the EET paper). The
     * decision is recorded for later {@link #replay}.
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
            record(noApplication());
            return expr; // rule 7 fallback: transform expression to itself
        }
        Application<E, T> application = randomApplication(Randomly.fromList(applicableRules), expr);
        record(application);
        return application.rule.apply(this, application, expr, false, false);
    }

    /**
     * Draws the random ingredients of one application of {@code rule} to {@code expr}. For the rules that use one, the
     * dead branch implements the paper's {@code rand_expr(type(expr))}: a random expression whose static type matches
     * that of {@code expr}. Although that expression is never evaluated, its static type participates in the DBMS's
     * CASE WHEN result-type resolution, so a type mismatch could alter the live branch's value or rendering. When the
     * type of {@code expr} cannot be inferred, no dead-branch expression is generated and the rule degenerates to the
     * {@code copy_expr} form of rules No. 5 and 6 (see {@link #deadBranch}).
     *
     * @param rule
     *            the transformation rule to draw an application of
     * @param expr
     *            the expression the application will wrap
     *
     * @return the drawn application
     */
    private Application<E, T> randomApplication(Rule rule, E expr) {
        if (rule.usesDeadBranch()) {
            T type = inferType(expr);
            E deadBranch = type == null ? null : generateExpressionOfType(type);
            return new Application<>(rule, generateBooleanExpression(), deadBranch, type);
        }
        return new Application<>(rule, generateBooleanExpression(), null, null);
    }

    /**
     * The dead branch of an application of rule No. 3 or 4 around the live expression {@code expr}: the recorded random
     * expression when it is kept and its type still matches the type inferred for {@code expr}, and {@code expr} itself
     * otherwise (the {@code copy_expr} degeneration, which trivially has the correct type). The types can stop matching
     * during {@link #replay}: disabling transformation sites inside {@code expr} may change its inferred type, and
     * reusing the recorded dead branch would then no longer be equivalence-preserving.
     *
     * @param application
     *            the application whose dead branch is built
     * @param expr
     *            the live expression the application wraps
     * @param copiedDeadBranch
     *            whether the recorded dead branch is discarded in favor of a copy of {@code expr}
     *
     * @return the dead-branch expression
     */
    private E deadBranch(Application<E, T> application, E expr, boolean copiedDeadBranch) {
        if (!copiedDeadBranch && application.deadBranch != null
                && Objects.equals(application.deadBranchType, inferType(expr))) {
            return application.deadBranch;
        }
        return expr;
    }

    /**
     * Transforms {@code expr} into a semantically equivalent expression. A transformation rule is always applied at the
     * root, guaranteeing (unless only rule 7 is applicable) that the returned expression differs from the input. The
     * rule applications performed are recorded and available via {@link #getLastTransformationRecord()}.
     *
     * @param expr
     *            the expression to transform
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value
     *
     * @return a semantically equivalent expression
     */
    public E transform(E expr, boolean booleanContext) {
        recording = new ArrayList<>();
        try {
            E transformed = transformNode(expr, booleanContext, true);
            lastRecord = new TransformationRecord(new ArrayList<>(recording));
            return transformed;
        } finally {
            recording = null;
        }
    }

    /**
     * Returns the record of the rule applications performed by the most recent {@link #transform} call, for later
     * {@link #replay}.
     *
     * @return the most recent transformation record, or {@code null} if {@link #transform} has not been called yet
     */
    public TransformationRecord getLastTransformationRecord() {
        return lastRecord;
    }

    /**
     * Re-applies a recorded transformation to {@code expr} (the same expression that was passed to the
     * {@link #transform} call that produced {@code record}), honoring the per-site {@code directives}: a disabled
     * application leaves its subexpression untransformed, and an enabled one may have its condition rendered as a
     * literal constant or its dead branch copied (see {@link SiteDirectives}). Because every recorded rule application
     * is individually equivalence-preserving under every directive combination, the returned expression is semantically
     * equivalent to {@code expr}, which makes replay suitable for test-case reduction: transformations are undone or
     * simplified one step at a time while the transformed query remains equivalent to the original.
     *
     * <p>
     * Replay walks the tree through the same {@link #descend} calls as the recording run, so it relies on
     * {@code descend} rebuilding nodes deterministically. No new random expressions are generated: all auxiliary
     * expressions are reused from the record.
     *
     * @param expr
     *            the expression the record's transform call originally transformed
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value (must match the original call)
     * @param record
     *            the record produced by this transformer's {@link #transform} call on {@code expr}
     * @param directives
     *            the per-site directives, consulted with site indices {@code 0} to {@code record.getSiteCount() - 1}
     *
     * @return the partially transformed expression
     */
    @SuppressWarnings("unchecked")
    public E replay(E expr, boolean booleanContext, TransformationRecord record, SiteDirectives directives) {
        replayApplications = new ArrayList<>();
        for (Application<?, ?> application : record.applications) {
            // safe: the record was produced by a transformer with the same type parameters
            replayApplications.add((Application<E, T>) application);
        }
        replayDirectives = directives;
        replayCursor = 0;
        replaySiteCursor = 0;
        try {
            E replayed = transformNode(expr, booleanContext, true);
            if (replayCursor != replayApplications.size()) {
                throw new IllegalStateException("The replay visited fewer nodes than the record contains");
            }
            return replayed;
        } finally {
            replayApplications = null;
            replayDirectives = null;
        }
    }

    /**
     * Descends into {@code expr}, rebuilds it from transformed children, then optionally applies a rule at this node
     * (or, during {@link #replay}, re-applies the recorded rule if its site is enabled).
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
        if (replayApplications != null) {
            return replayApplication(descended);
        }
        if (forceApply || Randomly.getBoolean()) {
            return applyRandomRule(descended, booleanContext);
        }
        record(noApplication());
        return descended;
    }

    /**
     * Consumes the next recorded decision and re-applies it to the rebuilt node (honoring the site's directives),
     * unless no rule was applied there or the application's site is disabled.
     *
     * @param descended
     *            the rebuilt node the decision applies to
     *
     * @return the (possibly wrapped) node
     */
    private E replayApplication(E descended) {
        if (replayCursor >= replayApplications.size()) {
            throw new IllegalStateException("The replay visited more nodes than the record contains");
        }
        Application<E, T> application = replayApplications.get(replayCursor++);
        if (application.rule == null) {
            return descended;
        }
        int site = replaySiteCursor++;
        if (!replayDirectives.isEnabled(site)) {
            return descended;
        }
        return application.rule.apply(this, application, descended, replayDirectives.useConstantCondition(site),
                replayDirectives.useCopiedDeadBranch(site));
    }

    private void record(Application<E, T> application) {
        if (recording != null) {
            recording.add(application);
        }
    }

    @SuppressWarnings("unchecked")
    private Application<E, T> noApplication() {
        return (Application<E, T>) Application.NONE;
    }

    /**
     * Rebuilds {@code expr} with its children transformed, threading the correct boolean/scalar context into each
     * child. Leaf nodes (columns, constants, table references, ...) should be returned unchanged; any applicable
     * transformation will still be applied to them by the calling {@link #transformNode}. Implementations must be
     * deterministic (in particular, visit the children of a given node in a fixed order), as {@link #replay} matches
     * recorded rule applications to nodes by their visiting order.
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
     * Builds a constant-TRUE boolean expression (e.g. the literal {@code TRUE}). Only used when a {@link #replay}
     * renders an always-true condition as a constant (see {@link SiteDirectives#useConstantCondition}).
     *
     * @return a constant-TRUE boolean expression
     */
    protected abstract E trueConstant();

    /**
     * Builds a constant-FALSE boolean expression (e.g. the literal {@code FALSE}). Only used when a {@link #replay}
     * renders an always-false condition as a constant (see {@link SiteDirectives#useConstantCondition}).
     *
     * @return a constant-FALSE boolean expression
     */
    protected abstract E falseConstant();

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
