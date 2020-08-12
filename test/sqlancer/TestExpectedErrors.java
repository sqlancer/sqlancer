package sqlancer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import sqlancer.common.query.ExpectedErrors;

public class TestExpectedErrors {

    @Test
    public void testEmpty() {
        ExpectedErrors errors = new ExpectedErrors();
        assertFalse(errors.errorIsExpected("a"));
    }

    @Test
    public void testSimple() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("a");
        errors.add("b");
        errors.add("c");
        assertTrue(errors.errorIsExpected("a"));
        assertTrue(errors.errorIsExpected("b"));
        assertTrue(errors.errorIsExpected("c"));
        assertTrue(errors.errorIsExpected("aa"));

        assertFalse(errors.errorIsExpected("d"));
    }

    @Test
    public void testRealistic() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("violated");
        assertTrue(errors.errorIsExpected("UNIQUE constraint was violated!"));
        assertTrue(errors.errorIsExpected("PRIMARY KEY constraint was violated!"));
    }

}
