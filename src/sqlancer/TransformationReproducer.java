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
     * Re-renders the transformed query with only the given transformation sites applied. Later
     * {@link #bugStillTriggers} calls and {@link #getBugInformation} use the re-rendered query.
     *
     * @param enabledSites
     *            the indices ({@code 0} to {@code getTransformationSiteCount() - 1}) of the sites to keep applied
     */
    void setEnabledTransformationSites(Set<Integer> enabledSites);
}
