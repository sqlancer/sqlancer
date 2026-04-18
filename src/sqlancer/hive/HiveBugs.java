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

    private HiveBugs() {
    }

}
