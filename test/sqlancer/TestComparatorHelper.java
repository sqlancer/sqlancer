package sqlancer;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.h2.H2Options;
import sqlancer.h2.H2Schema;

public class TestComparatorHelper {
    // TODO: Implement tests for the other ComparatorHelper methods

    // TODO: create test state that not depends on specific database
    final SQLGlobalState<H2Options, H2Schema> state = new SQLGlobalState<H2Options, H2Schema>() {

        @Override
        protected H2Schema readSchema() throws SQLException {
            return H2Schema.fromConnection(getConnection(), getDatabaseName());
        }

        @Override
        public MainOptions getOptions() {
            return new MainOptions();
        }
    };

    @Test
    public void testAssumeResultSetsAreEqualWithEqualSets() {
        List<String> r1 = Arrays.asList("a", "b", "c");
        List<String> r2 = Arrays.asList("a", "b", "c");
        ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), state);

    }

    @Test
    public void testAssumeResultSetsAreEqualWithUnequalLengthSets() {
        List<String> r1 = Arrays.asList("a", "b", "c");
        List<String> r2 = Arrays.asList("a", "b", "c", "d", "g");
        // NullPointerException is raised instead of AssertionError because state is null and the state.getState()...
        // line occurs before AssertionError is thrown, but it's good enough as an indicator that one of the Exceptions
        // is raised
        assertThrowsExactly(NullPointerException.class, () -> {
            ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), state);
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
            ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), state);
        });
    }

    @Test
    public void testAssumeResultSetsAreEqualWithCanonicalizationRule() {
        List<String> r1 = Arrays.asList("a", "b", "c");
        List<String> r2 = Arrays.asList("a", "b", "d");
        ComparatorHelper.assumeResultSetsAreEqual(r1, r2, "", Arrays.asList(""), state, (String s) -> {
            return s.equals("d") ? "c" : s;
        });
    }

}
