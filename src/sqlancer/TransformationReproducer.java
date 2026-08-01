package sqlancer;

import java.util.Set;

/**
 * A {@link Reproducer} for bugs found by comparing an original query against a transformed one, where the transformed
 * query was built by applying individually equivalence-preserving transformations (e.g. the EET rules) to the original.
 * Each such application is a transformation site, identified by an index that stays stable no matter which sites are
 * enabled. Disabling any subset of sites re-renders a transformed query that is still semantically equivalent to the
 * original, so {@link TransformationReducer} can soundly search for a minimal set of sites that still triggers the bug.
 *
 * @param <G>
 *            the DBMS-specific global state class
 */
public interface TransformationReproducer<G extends GlobalState<?, ?, ?>> extends Reproducer<G> {

    /**
     * The total number of transformation sites the transformed query was built with. This does not change when sites
     * are disabled.
     *
     * @return the total number of transformation sites
     */
    int getTransformationSiteCount();

    /**
     * The transformation sites whose rule application embeds a generated dead-branch expression, which
     * {@link #applyTransformationSites} may replace with a copy of the live expression.
     *
     * @return the indices of the sites with a generated dead branch
     */
    Set<Integer> getDeadBranchSites();

    /**
     * Re-renders the transformed query with only the given transformation sites applied, further simplified per site: a
     * site in {@code constantConditionSites} renders its always-true (or always-false) condition as the literal
     * constant of the same truth value, and a site in {@code copiedDeadBranchSites} replaces its generated dead branch
     * with a copy of the live expression. Both simplifications preserve the equivalence of the transformed query, like
     * disabling a site does. Later {@link #bugStillTriggers} calls and {@link #getBugInformation} use the re-rendered
     * query.
     *
     * @param enabledSites
     *            the indices ({@code 0} to {@code getTransformationSiteCount() - 1}) of the sites to keep applied
     * @param constantConditionSites
     *            the indices of the enabled sites whose condition is rendered as a literal constant
     * @param copiedDeadBranchSites
     *            the indices of the enabled sites whose dead branch is replaced by a copy of the live expression
     */
    void applyTransformationSites(Set<Integer> enabledSites, Set<Integer> constantConditionSites,
            Set<Integer> copiedDeadBranchSites);
}
