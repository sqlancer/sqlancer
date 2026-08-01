package sqlancer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import sqlancer.common.query.Query;

/**
 * Reduces the transformed query of a {@link TransformationReproducer} by disabling transformation sites, searching
 * (with the same delta-debugging strategy as {@link StatementReducer}) for a minimal set of sites that still triggers
 * the bug. Because each site is an individually equivalence-preserving rewrite, any subset of sites yields a
 * transformed query that is still semantically equivalent to the original query, so the reduction is sound. This
 * reducer runs after statement reduction, evaluating each candidate against the already-reduced database; for
 * reproducers that do not implement {@link TransformationReproducer}, it does nothing.
 *
 * @param <G>
 *            the DBMS-specific global state class
 * @param <O>
 *            the DBMS-specific options class
 * @param <C>
 *            the DBMS-specific connection class
 */
public class TransformationReducer<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection>
        implements Reducer<G> {

    private final DatabaseProvider<G, O, C> provider;
    private List<Query<C>> statements;
    private boolean observedChange;
    private int partitionNum;

    private long currentReduceSteps;
    private long currentReduceTime;

    private long maxReduceSteps;
    private long maxReduceTime;

    private Instant timeOfReductionBegins;

    public TransformationReducer(DatabaseProvider<G, O, C> provider) {
        this.provider = provider;
    }

    private boolean hasNotReachedLimit(long curr, long limit) {
        if (limit == MainOptions.NO_REDUCE_LIMIT) {
            return true;
        }
        return curr < limit;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(G state, Reproducer<G> reproducer, G newGlobalState) throws Exception {
        if (!(reproducer instanceof TransformationReproducer)) {
            return;
        }
        TransformationReproducer<G> transformationReproducer = (TransformationReproducer<G>) reproducer;

        maxReduceTime = state.getOptions().getMaxStatementReduceTime();
        maxReduceSteps = state.getOptions().getMaxStatementReduceSteps();

        // Snapshot the (already reduced) generation statements once: createDatabase logs its setup statements
        // (DROP/CREATE/USE) into the state, so the state's statement list must be reset for every candidate rather
        // than read back, lest the setup statements accumulate and get re-executed mid-test-case.
        statements = new ArrayList<>();
        for (Query<?> stat : newGlobalState.getState().getStatements()) {
            statements.add((Query<C>) stat);
        }

        List<Integer> enabledSites = new ArrayList<>();
        for (int site = 0; site < transformationReproducer.getTransformationSiteCount(); site++) {
            enabledSites.add(site);
        }
        // With every site disabled the transformed query renders as the original one, which cannot mismatch with
        // itself, so a single remaining site cannot be reduced further.
        if (enabledSites.size() < 2) {
            return;
        }

        timeOfReductionBegins = Instant.now();
        currentReduceSteps = 0;
        currentReduceTime = 0;
        partitionNum = 2;

        while (enabledSites.size() >= 2 && hasNotReachedLimit(currentReduceSteps, maxReduceSteps)
                && hasNotReachedLimit(currentReduceTime, maxReduceTime)) {
            observedChange = false;

            enabledSites = tryReduction(transformationReproducer, newGlobalState, enabledSites);

            if (!observedChange) {
                if (partitionNum == enabledSites.size()) {
                    break;
                }
                // increase the search granularity
                partitionNum = Math.min(partitionNum * 2, enabledSites.size());
            }
        }

        // Leave the reproducer holding the reduced transformed query (the last candidate tried may have failed), so
        // the final bug information reflects the reduction.
        transformationReproducer.setEnabledTransformationSites(new HashSet<>(enabledSites));
        newGlobalState.getState().setStatements(new ArrayList<>(statements));
        newGlobalState.getLogger().updateReducedBugInformation(transformationReproducer.getBugInformation());
        newGlobalState.getLogger().logReduced(newGlobalState.getState(),
                "Transformation reduction finished; the transformed query was reduced to the one shown below");
    }

    private List<Integer> tryReduction(TransformationReproducer<G> transformationReproducer, G newGlobalState,
            List<Integer> enabledSites) throws Exception {

        List<Integer> sites = enabledSites;

        int start = 0;
        int subLength = sites.size() / partitionNum;
        while (start < sites.size()) {
            // candidateSites = sites[:start] + sites[start+subLength:]
            // in other words, remove [start, start+subLength) from sites
            List<Integer> candidateSites = new ArrayList<>(sites);
            int endPoint = Math.min(start + subLength, candidateSites.size());
            candidateSites.subList(start, endPoint).clear();

            if (bugStillTriggersWith(transformationReproducer, newGlobalState, candidateSites)) {
                observedChange = true;
                sites = candidateSites;
                partitionNum = Math.max(partitionNum - 1, 2);
                newGlobalState.getLogger().updateReducedBugInformation(transformationReproducer.getBugInformation());
                newGlobalState.getLogger().logReduced(newGlobalState.getState());
                break;
            }

            currentReduceSteps++;
            currentReduceTime = Duration.between(timeOfReductionBegins, Instant.now()).getSeconds();
            if (!hasNotReachedLimit(currentReduceSteps, maxReduceSteps)
                    || !hasNotReachedLimit(currentReduceTime, maxReduceTime)) {
                return sites;
            }
            start = start + subLength;
        }
        return sites;
    }

    /**
     * Whether the bug still triggers with only {@code candidateSites} applied to the transformed query, evaluated
     * against a freshly recreated database populated with the (already reduced) generation statements.
     *
     * @param transformationReproducer
     *            the reproducer whose transformed query is being reduced
     * @param newGlobalState
     *            the state the candidate is evaluated against
     * @param candidateSites
     *            the transformation sites to keep applied
     *
     * @return {@code true} if the bug still triggers with the candidate sites
     */
    private boolean bugStillTriggersWith(TransformationReproducer<G> transformationReproducer, G newGlobalState,
            List<Integer> candidateSites) {
        transformationReproducer.setEnabledTransformationSites(new HashSet<>(candidateSites));
        try (C con2 = provider.createDatabase(newGlobalState)) {
            newGlobalState.setConnection(con2);
            // discard the setup statements createDatabase just logged into the state
            newGlobalState.getState().setStatements(new ArrayList<>(statements));
            for (Query<C> s : statements) {
                try {
                    s.execute(newGlobalState);
                } catch (Throwable ignoredException) {
                    // ignore
                }
            }
            try {
                return transformationReproducer.bugStillTriggers(newGlobalState);
            } catch (Throwable ignoredException) {
                // fall through: this candidate no longer triggers the bug
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
