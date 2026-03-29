package sqlancer.questdb;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import sqlancer.common.query.ExpectedErrors;

public final class QuestDBErrors {

    private QuestDBErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // TODO (anxing)
        errors.add("unexpected argument for function: ");
        errors.add("unexpected token:"); // SELECT FROM multiple tables without WHERE/ JOIN clause
        errors.add("boolean expression expected");
        errors.add("Column name expected");
        errors.add("too few arguments for 'in'");
        errors.add("cannot compare TIMESTAMP with type"); // WHERE column IN with nonTIMESTAMP arg
        errors.add("constant expected");
        errors.add("Invalid date");
        errors.add("invalid timestamp");
        errors.add("invalid column");
        errors.add("Invalid column");
        errors.add("expression type mismatch");
        errors.add("inconvertible value");
        errors.add("not implemented: dynamic pattern would be very slow to execute");

        if (QuestDBBugs.bug4981) {
            errors.add("Cannot invoke \"java.lang.CharSequence.length()\" because \"seq\" is null");
        }

        return errors;
    }

    public static List<Pattern> getExpressionErrorsRegex() {
        ArrayList<Pattern> errors = new ArrayList<>();

        errors.add(Pattern.compile("argument type mismatch for function `.+`"));
        errors.add(Pattern.compile("there is no matching operator`.+` with the argument types"));
        errors.add(Pattern.compile("there is no matching operator `.+` with the argument type"));
        errors.add(Pattern.compile("cannot compare \\w+ with type \\w+"));
        errors.add(Pattern.compile("invalid .+ type"));
        errors.add(Pattern.compile("invalid .+ value"));
        errors.add(Pattern.compile("invalid .+ format"));

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
        errors.addAllRegexes(getExpressionErrorsRegex());
    }

    public static List<String> getGroupByErrors() {
        // TODO (anxing)

        return new ArrayList<>();
    }

    public static void addGroupByErrors(ExpectedErrors errors) {
        errors.addAll(getGroupByErrors());
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // TODO (anxing)
        errors.add("Invalid column");
        errors.add("inconvertible types:");
        errors.add("inconvertible value:");

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }
}
