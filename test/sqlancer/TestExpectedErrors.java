package sqlancer;

import java.util.regex.Pattern;

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
    public void testStringSimple() {
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
    public void testRegexSimple() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addRegex(Pattern.compile("a\\d"));
        errors.addRegex(Pattern.compile("b\\D"));
        errors.add("c");
        assertTrue(errors.errorIsExpected("a0"));
        assertTrue(errors.errorIsExpected("bb"));
        assertTrue(errors.errorIsExpected("c"));
        assertFalse(errors.errorIsExpected("aa"));

    }

    @Test
    public void testStringRealistic() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("violated");
        assertTrue(errors.errorIsExpected("UNIQUE constraint was violated!"));
        assertTrue(errors.errorIsExpected("PRIMARY KEY constraint was violated!"));
    }

    @Test
    public void testRegexRealistic() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addRegex(Pattern.compile(".violated."));
        assertTrue(errors.errorIsExpected("UNIQUE constraint was violated!"));
        assertTrue(errors.errorIsExpected("PRIMARY KEY constraint was violated!"));
    }

}
