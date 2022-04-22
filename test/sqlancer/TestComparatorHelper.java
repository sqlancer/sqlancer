package sqlancer;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class TestComparatorHelper {
    // TODO: Implement tests for the other ComparatorHelper methods

    @Test
    public void testAssumeResultSetsAreEqualWithEqualSets() {
        List<String> r1 = Arrays.asList("a", "b", "c");
        List<String> r2 = Arrays.asList("a", "b", "c");
        ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), null);

    }

    @Test
    public void testAssumeResultSetsAreEqualWithUnequalLengthSets() {
        List<String> r1 = Arrays.asList("a", "b", "c");
        List<String> r2 = Arrays.asList("a", "b", "c", "d", "g");
        // NullPointerException is raised instead of AssertionError because state is null and the state.getState()...
        // line occurs before AssertionError is thrown, but it's good enough as an indicator that one of the Exceptions
        // is raised
        assertThrowsExactly(NullPointerException.class, () -> {
            ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), null);
        });
    }

    @Test
    public void testAssumeResultSetsAreEqualWithUnequalValueSets() {
        List<String> r1 = Arrays.asList("a", "b", "c");
        List<String> r2 = Arrays.asList("a", "b", "d");
        // NullPointerException is raised instead of AssertionError because state is null and the state.getState()...
        // line occurs before AssertionError is thrown, but it's good enough as an indicator that one of the Exceptions
        // is raised
        assertThrowsExactly(NullPointerException.class, () -> {
            ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), null);
        });
    }

    @Test
    public void testAssumeResultSetsAreEqualWithCanonicalizationRule() {
        List<String> r1 = Arrays.asList("a", "b", "c");
        List<String> r2 = Arrays.asList("a", "b", "d");
        ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), null, (String s) -> {
            return s.equals("d") ? "c" : s;
        });
    }

}
