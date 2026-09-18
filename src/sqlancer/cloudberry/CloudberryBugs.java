package sqlancer.cloudberry;

// do not make the fields final to avoid warnings
public final class CloudberryBugs {

    // A GPORCA coordinator crash (SIGSEGV), not just a controlled error: CExtendedStatsProcessor::
    // ApplyCorrelatedStatsToScaleFactorFilterCalculation (CExtendedStatsProcessor.cpp) dereferences
    // the result of colid_to_attno_mapping->Find(&colid) without checking for null. Reachable when a
    // table has a "CREATE STATISTICS ... (dependencies) ON <subset of columns>" extended-stats object
    // and a conjunctive filter (through a subquery/alias boundary, e.g. GROUP BY + an outer WHERE on
    // the derived columns) includes a clause on a column outside that subset -- Find() legitimately
    // returns null for that clause, and the code dereferences it anyway. Unlike a normal expected
    // error, this kills the coordinator backend and can cascade into unrelated concurrent sessions
    // ("database system is in recovery mode"), so it cannot be handled by whitelisting an error
    // message -- CREATE_STATISTICS generation is disabled outright in CloudberryProvider while true.
    public static boolean bugExtendedStatsDependenciesSegfault = true;

    // A known GPORCA bug (CPhysicalFullMergeJoin::PdsDerive mis-deriving distribution for a FULL MERGE
    // JOIN with a Universal-distributed outer child) already fixed locally but not yet merged into the
    // Cloudberry main branch this is built from, so it can still surface against this cluster.
    public static boolean bugFullMergeJoinUnexpectedGangSize = true;

    // A GPORCA bug: an INCLUDE (non-key) index column referenced by a predicate is treated as a valid
    // index key column when building an Index Cond (likely via CXformUtils::PdrgpcrIndexColumns, which
    // concatenates key and included columns into one array with no marker distinguishing the two). The
    // resulting plan carries a scan key against an attribute beyond indnkeyatts, which the executor
    // rejects. Which sanity check fires depends on the predicate, so one bug produces two error
    // messages -- both are suppressed below:
    //
    // CREATE TABLE t(a int, b <type>); CREATE INDEX i ON t USING btree (a) INCLUDE (b);
    // SET optimizer = on;
    // SELECT a FROM t WHERE b; -- b boolean: "bogus index qualification (nodeIndexscan.c)"
    // SELECT a FROM t WHERE b IS NULL; -- b int: "btree index keys must be ordered by attribute"
    //
    // Verified scope: optimizer=on only (the Postgres planner never builds such an Index Cond, and
    // every case above returns the correct rows under optimizer=off); not specific to a distribution
    // policy (DISTRIBUTED BY and DISTRIBUTED REPLICATED both reproduce); needs no data, an empty table
    // is enough; and the INCLUDE list does not have to overlap the key columns -- a plain
    // "INCLUDE (b)" is sufficient. A fix exists locally but has not been merged into the Cloudberry
    // main branch this environment is built from.
    public static boolean bugIndexCondOnIncludedColumn = true;

    // A real, still-open planner bug, not Cloudberry-specific: clauselist_selectivity()'s OR-clause
    // combinator in clausesel.c ("s1 = s1 + s2 - s1 * s2") never re-clamps its result to [0, 1] before
    // returning it. With a sparse ANALYZE sample, a range-ish predicate's selectivity estimate can drift
    // a hair above 1.0, and that out-of-range value later blows the unrelated postcondition
    // Assert(pselec >= 0.0 && pselec <= 1.0) in adjust_selectivity_for_nulltest() (costsize.c:5436),
    // which runs for any outer join regardless of whether the qual is actually a null test.
    // Assert-build-only (production builds would just silently keep the slightly-wrong selectivity);
    // minimal repro and analysis captured, fix not yet applied upstream. Unlike the two bugs above
    // (both plain elog(ERROR), which only abort the current transaction), this is a hard abort() that
    // kills the whole backend process and forces a coordinator-wide restart -- every other
    // concurrently-running thread's connection briefly fails too (self-recovers within a few seconds,
    // no manual action needed).
    //
    // Minimal repro (needs ANALYZE; assert-build only):
    // CREATE TABLE m1(c0 inet); CREATE TABLE m2(c0 inet);
    // INSERT INTO m2 VALUES ('88.147.138.141'), ('76.163.212.11'), ('214.10.65.144');
    // ANALYZE m1, m2;
    // SELECT COUNT(*) FROM ONLY m1 LEFT OUTER JOIN m2 ON true
    // WHERE (m1.c0 IS NOT NULL) OR (m2.c0 BETWEEN SYMMETRIC '75.175.243.19' AND '230.9.216.68');
    //
    // A fix already has an MR open against Cloudberry (as of 2026-09-16). Until it merges and this
    // environment is rebuilt against it, CloudberryCommon suppresses only the exact assertion
    // signature above ("pselec >= 0.0 && pselec <= 1.0") -- an unambiguous fingerprint of this one
    // bug -- and deliberately leaves the bug's generic connection-loss fallout ("connection has been
    // closed", "I/O error...", "recovery mode", etc.) unfiltered, since that wording could just as
    // easily be a different, not-yet-diagnosed crash. Flip this flag to false once the fix lands.
    public static boolean bugOrSelectivityNotClampedAssert = true;

    // REINDEX ... CONCURRENTLY against an ao_column (AOCO) table raises the raw internal error
    // "not implemented yet (aocsam_handler.c:2011)" instead of a clean user-facing "not supported"
    // message. That elog is the body of aoco_index_validate_scan(), an unimplemented table-AM callback
    // in src/backend/access/aocs/aocsam_handler.c -- CONCURRENTLY's two-phase index build reaches the
    // validate_scan path, which AOCO never implemented. Reached whenever the statement generator
    // emits "REINDEX DATABASE CONCURRENTLY". Needs no data, and
    // the controls confirm the scope is exactly AOCO + CONCURRENTLY: the same REINDEX without
    // CONCURRENTLY succeeds, as does REINDEX CONCURRENTLY on a heap table.
    //
    // Minimal repro:
    // CREATE TABLE ao1(a int, b int) USING ao_column DISTRIBUTED BY (a);
    // CREATE INDEX ao1_i ON ao1(a);
    // REINDEX TABLE CONCURRENTLY ao1;
    //
    // The suppressed string is scoped to the source file rather than the bare "not implemented yet"
    // (far too broad -- it would hide every other unimplemented callback in the tree) and rather than
    // the exact "aocsam_handler.c:2011" (brittle: the line number moves whenever that file is edited).
    // File-level scoping keeps it narrow to AOCO's unimplemented table-AM callbacks.
    public static boolean bugAocoReindexConcurrentlyNotImplemented = true;

    private CloudberryBugs() {
    }

}
