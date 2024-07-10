package sqlancer;

import java.util.List;
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
    public void testStringsSimple() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addAll(List.of("a", "b", "c"));
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
    public void testRegexesSimple() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addAllRegexes(List.of(Pattern.compile("a\\d"), Pattern.compile("b\\D")));
        errors.add("c");
        assertTrue(errors.errorIsExpected("a0"));
        assertTrue(errors.errorIsExpected("bb"));
        assertTrue(errors.errorIsExpected("c"));
        assertFalse(errors.errorIsExpected("aa"));
    }

    @Test
    public void testRegexStringSimple() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addRegexString("a\\d");
        errors.addRegexString("b\\D");
        errors.add("c");
        assertTrue(errors.errorIsExpected("a0"));
        assertTrue(errors.errorIsExpected("bb"));
        assertTrue(errors.errorIsExpected("c"));
        assertFalse(errors.errorIsExpected("aa"));

    }

    @Test
    public void testRegexStrings() {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addAllRegexStrings(List.of("a\\d", "b\\D"));
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

    @Test
    public void testBuilder() {
        ExpectedErrors errors = ExpectedErrors.newErrors().with("a", "b", "c").build();

        assertTrue(errors.errorIsExpected("a"));
        assertTrue(errors.errorIsExpected("b"));
        assertTrue(errors.errorIsExpected("c"));
        assertTrue(errors.errorIsExpected("aa"));
        assertFalse(errors.errorIsExpected("d"));

        errors = ExpectedErrors.newErrors().with(List.of("a", "b", "c")).build();

        assertTrue(errors.errorIsExpected("a"));
        assertTrue(errors.errorIsExpected("b"));
        assertTrue(errors.errorIsExpected("c"));
        assertTrue(errors.errorIsExpected("aa"));
        assertFalse(errors.errorIsExpected("d"));

        errors = ExpectedErrors.newErrors().withRegexString("a\\d", "b\\D").with("c").build();

        assertTrue(errors.errorIsExpected("a0"));
        assertTrue(errors.errorIsExpected("bb"));
        assertTrue(errors.errorIsExpected("c"));
        assertFalse(errors.errorIsExpected("aa"));

        errors = ExpectedErrors.newErrors().withRegexString(List.of("a\\d", "b\\D")).with("c").build();

        assertTrue(errors.errorIsExpected("a0"));
        assertTrue(errors.errorIsExpected("bb"));
        assertTrue(errors.errorIsExpected("c"));
        assertFalse(errors.errorIsExpected("aa"));

        errors = ExpectedErrors.newErrors().withRegex(Pattern.compile("a\\d"), Pattern.compile("b\\D")).with("c")
                .build();

        assertTrue(errors.errorIsExpected("a0"));
        assertTrue(errors.errorIsExpected("bb"));
        assertTrue(errors.errorIsExpected("c"));
        assertFalse(errors.errorIsExpected("aa"));

        errors = ExpectedErrors.newErrors().withRegex(List.of(Pattern.compile("a\\d"), Pattern.compile("b\\D")))
                .with("c").build();

        assertTrue(errors.errorIsExpected("a0"));
        assertTrue(errors.errorIsExpected("bb"));
        assertTrue(errors.errorIsExpected("c"));
        assertFalse(errors.errorIsExpected("aa"));
    }
}
