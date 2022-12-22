package sqlancer.common.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This class represents the errors that executing a statement might result in. For example, an INSERT statement might
 * result in an error "UNIQUE constraint violated" when it attempts to insert a duplicate value in a column declared as
 * UNIQUE.
 */
public class ExpectedErrors {

    private final Set<String> errors = new HashSet<>();
    private final List<Pattern> regexes = new ArrayList<>();

    public ExpectedErrors add(String error) {
        if (error == null) {
            throw new IllegalArgumentException();
        }
        errors.add(error);
        return this;
    }

    public ExpectedErrors addRegex(Pattern errorPattern) {
        if (errorPattern == null) {
            throw new IllegalArgumentException();
        }
        regexes.add(errorPattern);
        return this;
    }

    public ExpectedErrors addAll(Collection<String> list) {
        errors.addAll(list);
        return this;
    }

    public ExpectedErrors addAllRegexes(Collection<Pattern> list) {
        if (list == null) {
            throw new IllegalArgumentException();
        }
        regexes.addAll(list);
        return this;
    }

    public static ExpectedErrors from(String... errors) {
        ExpectedErrors expectedErrors = new ExpectedErrors();
        for (String error : errors) {
            expectedErrors.add(error);
        }
        return expectedErrors;
    }

    /**
     * Checks whether the error message (e.g., returned by the DBMS under test) contains any of the added error
     * messages.
     *
     * @param error
     *            the error message
     *
     * @return whether the error message contains any of the substrings specified as expected errors
     */
    public boolean errorIsExpected(String error) {
        if (error == null) {
            throw new IllegalArgumentException();
        }
        for (String s : this.errors) {
            if (error.contains(s)) {
                return true;
            }
        }
        for (Pattern p : this.regexes) {
            if (p.matcher(error).find()) {
                return true;
            }
        }
        return false;
    }

}
