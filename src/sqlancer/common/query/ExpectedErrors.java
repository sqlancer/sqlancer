package sqlancer.common.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This class represents the errors that executing a statement might result in. For example, an INSERT statement might
 * result in an error "UNIQUE constraint violated" when it attempts to insert a duplicate value in a column declared as
 * UNIQUE.
 */
public class ExpectedErrors {

    private final Set<String> errors;
    private final Set<Pattern> regexes;

    public ExpectedErrors() {
        this.errors = new HashSet<>();
        this.regexes = new HashSet<>();
    }

    public ExpectedErrors(Collection<String> errors, Collection<Pattern> regexErrors) {
        this.errors = new HashSet<>(errors);
        this.regexes = new HashSet<>(regexErrors);
    }

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

    public ExpectedErrors addRegexString(String errorPattern) {
        if (errorPattern == null) {
            throw new IllegalArgumentException();
        }
        regexes.add(Pattern.compile(errorPattern));
        return this;
    }

    public ExpectedErrors addAll(Collection<String> list) {
        if (list == null) {
            throw new IllegalArgumentException();
        }
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

    public ExpectedErrors addAllRegexStrings(Collection<String> list) {
        for (String error : list) {
            regexes.add(Pattern.compile(error));
        }
        return this;
    }

    public static ExpectedErrors from(String... errors) {
        return newErrors().with(errors).build();
    }

    public static ExpectedErrorsBuilder newErrors() {
        return new ExpectedErrorsBuilder();
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

    public static class ExpectedErrorsBuilder {
        private final Set<String> errors = new HashSet<>();
        private final Set<Pattern> regexes = new HashSet<>();

        public ExpectedErrorsBuilder with(String... list) {
            errors.addAll(Arrays.asList(list));
            return this;
        }

        public ExpectedErrorsBuilder with(Collection<String> list) {
            return with(list.toArray(new String[0]));
        }

        public ExpectedErrorsBuilder withRegex(Pattern... list) {
            regexes.addAll(Arrays.asList(list));
            return this;
        }

        public ExpectedErrorsBuilder withRegex(Collection<Pattern> list) {
            return withRegex(list.toArray(new Pattern[0]));
        }

        public ExpectedErrorsBuilder withRegexString(String... list) {
            for (String error : list) {
                regexes.add(Pattern.compile(error));
            }
            return this;
        }

        public ExpectedErrorsBuilder withRegexString(Collection<String> list) {
            return withRegexString(list.toArray(new String[0]));
        }

        public ExpectedErrors build() {
            return new ExpectedErrors(errors, regexes);
        }
    }
}
