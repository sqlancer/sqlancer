package sqlancer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.common.query.Query;

/**
 * Reduces the transformed query of a {@link TransformationReproducer} in two phases. First, transformation sites are
 * disabled with the same delta-debugging strategy as {@link StatementReducer}, searching for a minimal set of sites
 * that still triggers the bug. Second, each surviving site is greedily simplified: its always-true (or always-false)
 * condition is rendered as a literal constant, and its generated dead branch is replaced by a copy of the live
 * expression, keeping each simplification only if the bug still triggers. Because each site is an individually
 * equivalence-preserving rewrite and both simplifications preserve that property, every candidate transformed query
 * remains semantically equivalent to the original query, so the reduction is sound. This reducer runs after statement
 * reduction, evaluating each candidate against the already-reduced database; for reproducers that do not implement
 * {@link TransformationReproducer}, it does nothing.
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

    private Set<Integer> constantConditionSites;
    private Set<Integer> copiedDeadBranchSites;

    public TransformationReducer(DatabaseProvider<G, O, C> provider) {
        this.provider = provider;
    }

    private boolean hasNotReachedLimit(long curr, long limit) {
        if (limit == MainOptions.NO_REDUCE_LIMIT) {
            return true;
        }
        return curr < limit;
    }

    private boolean withinLimits() {
        return hasNotReachedLimit(currentReduceSteps, maxReduceSteps)
                && hasNotReachedLimit(currentReduceTime, maxReduceTime);
    }

    // Accounts one candidate evaluation against the step/time limits; returns whether reduction may continue.
    private boolean registerStepAndCheckLimits() {
        currentReduceSteps++;
        currentReduceTime = Duration.between(timeOfReductionBegins, Instant.now()).getSeconds();
        return withinLimits();
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
        if (enabledSites.isEmpty()) {
            return;
        }

        timeOfReductionBegins = Instant.now();
        currentReduceSteps = 0;
        currentReduceTime = 0;
        partitionNum = 2;
        constantConditionSites = new HashSet<>();
        copiedDeadBranchSites = new HashSet<>();

        // Phase 1: delta-debug the enabled-site set. With every site disabled the transformed query renders as the
        // original one, which cannot mismatch with itself, so a single remaining site is not removable further.
        while (enabledSites.size() >= 2 && withinLimits()) {
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

        simplifySurvivingSites(transformationReproducer, newGlobalState, enabledSites);

        // Leave the reproducer holding the reduced transformed query (the last candidate tried may have failed), so
        // the final bug information reflects the reduction.
        transformationReproducer.applyTransformationSites(new HashSet<>(enabledSites), constantConditionSites,
                copiedDeadBranchSites);
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
                logReductionStep(transformationReproducer, newGlobalState);
                break;
            }

            if (!registerStepAndCheckLimits()) {
                return sites;
            }
            start = start + subLength;
        }
        return sites;
    }

    /**
     * Phase 2: greedily simplifies each surviving site, keeping a simplification only if the bug still triggers. First
     * the site's condition is rendered as a literal constant (the condition's embedded random predicate is often the
     * bulk of the transformed query), then, for sites that have one, the generated dead branch is replaced by a copy of
     * the live expression.
     *
     * @param transformationReproducer
     *            the reproducer whose transformed query is being reduced
     * @param newGlobalState
     *            the state the candidates are evaluated against
     * @param enabledSites
     *            the sites that survived phase 1
     */
    private void simplifySurvivingSites(TransformationReproducer<G> transformationReproducer, G newGlobalState,
            List<Integer> enabledSites) {
        Set<Integer> deadBranchSites = transformationReproducer.getDeadBranchSites();
        for (int site : enabledSites) {
            if (!withinLimits()) {
                return;
            }
            constantConditionSites.add(site);
            if (bugStillTriggersWith(transformationReproducer, newGlobalState, enabledSites)) {
                logReductionStep(transformationReproducer, newGlobalState);
            } else {
                constantConditionSites.remove(site);
            }
            if (!registerStepAndCheckLimits()) {
                return;
            }

            if (deadBranchSites.contains(site)) {
                copiedDeadBranchSites.add(site);
                if (bugStillTriggersWith(transformationReproducer, newGlobalState, enabledSites)) {
                    logReductionStep(transformationReproducer, newGlobalState);
                } else {
                    copiedDeadBranchSites.remove(site);
                }
                if (!registerStepAndCheckLimits()) {
                    return;
                }
            }
        }
    }

    // Logs an accepted reduction step, refreshing the logged bug information with the re-rendered transformed query.
    private void logReductionStep(TransformationReproducer<G> transformationReproducer, G newGlobalState) {
        newGlobalState.getLogger().updateReducedBugInformation(transformationReproducer.getBugInformation());
        newGlobalState.getLogger().logReduced(newGlobalState.getState());
    }

    /**
     * Whether the bug still triggers with the given sites applied to the transformed query (further simplified per the
     * current constant-condition and copied-dead-branch sets), evaluated against a freshly recreated database populated
     * with the (already reduced) generation statements.
     *
     * @param transformationReproducer
     *            the reproducer whose transformed query is being reduced
     * @param newGlobalState
     *            the state the candidate is evaluated against
     * @param candidateSites
     *            the transformation sites to keep applied
     *
     * @return {@code true} if the bug still triggers with the candidate configuration
     */
    private boolean bugStillTriggersWith(TransformationReproducer<G> transformationReproducer, G newGlobalState,
            List<Integer> candidateSites) {
        transformationReproducer.applyTransformationSites(new HashSet<>(candidateSites), constantConditionSites,
                copiedDeadBranchSites);
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
