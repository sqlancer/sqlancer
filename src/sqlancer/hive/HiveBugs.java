package sqlancer.hive;

// do not make the fields final to avoid warnings
public final class HiveBugs {

    // Incorrect IS NULL evaluation for negation of string concatenation involving column references.
    // -(c || 'x') evaluates to NULL at runtime, but IS NULL incorrectly returns false.
    // The optimizer's nullability inference for GenericUDFOPNegative does not account for
    // runtime conversion failures producing NULL from non-null input.
    // Reproduce: CREATE TABLE t(c DOUBLE); INSERT INTO t VALUES(1.0);
    // SELECT (-(c || 'x')) IS NULL FROM t; -- returns false, expected true
    // Affects: 4.0.1, 4.2.0
    public static boolean bugNegationNullability = true;

    // Non-boolean expressions (CAST to non-boolean, FLOOR, ROUND, arithmetic) silently
    // return 0 rows for all three TLP partitions when used as WHERE predicates.
    // Hive requires BOOLEAN in WHERE but does not error; instead it returns empty results.
    // Reproduce: CREATE TABLE t(c INT); INSERT INTO t VALUES(1);
    // SELECT * FROM t WHERE FLOOR(1); -- returns 0 rows, expected 1
    // Affects: 4.0.1, 4.2.0
    public static boolean bugNonBooleanWhereClause = true;

    // IN operator with boolean sub-expressions involving IS NULL evaluates incorrectly,
    // returning 0 rows for all three TLP partitions.
    // Reproduce: CREATE TABLE t(c BOOLEAN); INSERT INTO t VALUES(true),(false);
    // SELECT * FROM t WHERE (c != c) IN ((false) IS NULL); -- returns 0, expected 2
    // Affects: 4.0.1, 4.2.0
    public static boolean bugInBooleanEvaluation = true;

    // BETWEEN with mixed boolean/numeric types has incorrect TLP evaluation.
    // The IS NULL partition misses rows due to wrong nullability inference.
    // Reproduce: CREATE TABLE t(c DOUBLE); INSERT INTO t VALUES(0.5),(1.5);
    // SELECT * FROM t WHERE (c NOT IN (true)) NOT BETWEEN 0.01 AND c;
    // -- TLP partitions lose rows
    // Affects: 4.0.1, 4.2.0
    public static boolean bugBetweenMixedTypes = true;

    private HiveBugs() {
    }

}
