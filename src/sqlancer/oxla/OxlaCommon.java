package sqlancer.oxla;

import sqlancer.common.query.ExpectedErrors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class OxlaCommon {
    private OxlaCommon() {
    }

    public static final List<String> EXACT_ERRORS = List.of(
            "'*' can be used only in the SELECT clause.",
            "Expected frame clause with ROWS or RANGE mode",
            "LIMIT must not be negative",
            "Not all provided names_to_validate were found in the schema",
            "OFFSET must not be negative",
            "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column",
            "SELECT ... INTO table is not supported",
            "aggregate function calls cannot be nested",
            "argument of JOIN/ON must be type boolean, not type integer",
            "argument of RANGE must not contain variables",
            "argument of ROWS must be an expression foldable to a bigint literal",
            "argument of nth_value must be greater than zero",
            "both sides of \"=\" operator in JOIN ON condition must come from different sources",
            "cannot get array length of a non-array",
            "could not determine polymorphic type because input has type unknown",
            "division by zero",
            "empty format provided",
            "empty input provided",
            "expecting only literal for percentiles",
            "expression on one side of \"=\" operator in JOIN ON condition must come from exactly one source",
            "frame end cannot be UNBOUNDED PRECEDING",
            "frame start cannot be UNBOUNDED FOLLOWING",
            "frame starting from current row cannot have preceding rows",
            "frame starting from following row cannot end with current row",
            "frame starting from following row cannot have preceding rows",
            "invalid JOIN ON clause condition. Only equi join is supported",
            "invalid input syntax for type",
            "non-integer constant in GROUP BY",
            "non-integer constant in ORDER BY",
            "out of range",
            "zero raised to a negative power is undefined"
    );
    public static final List<Pattern> REGEX_ERRORS = List.of(
            Pattern.compile("Could not locate this time zone:.*"),
            Pattern.compile("Failed to compile '[^']+' as a regular expression pattern"),
            Pattern.compile("Function \\S+ is not window function"),
            Pattern.compile("GROUP BY position (\\d+) is not in select list"),
            Pattern.compile("HAS_SCHEMA_PRIVILEGE function got unrecognized privilege type:\\s+\"[^\"]*\""),
            Pattern.compile("ORDER BY position (\\d+) is not in select list"),
            Pattern.compile("RANGE with offset PRECEDING/FOLLOWING is not supported for column type (.*)"),
            Pattern.compile("\\S+ types .*?(?=and)and .*?(?=cannot)cannot be matched"),
            Pattern.compile("aggregate functions are not allowed in (.+)"),
            Pattern.compile("argument of ROWS must be type bigint, not type (.+)"),
            Pattern.compile("cannot copy window \"[^\"]+\" because it has a frame clause"),
            Pattern.compile("cannot implicitly cast from .*?(?=to)to (.*)"),
            Pattern.compile("cannot override \\S+ BY clause of window \"[^\"]+\""),
            Pattern.compile("column \"[^\"]*\" does not exist"),
            Pattern.compile("column \"[^\"]*\" must appear in the GROUP BY clause or be used in an aggregate function"),
            Pattern.compile("column reference \"[^\"]*\" is ambiguous"),
            Pattern.compile("could not identify an \\S+ operator for type\\s+.*"),
            Pattern.compile("database \"[^\"]*\" does not exist"),
            Pattern.compile("each \\S+ query must have the same number of columns"),
            Pattern.compile("for SELECT DISTINCT, expression \"[^\"]*\" must appear in select list"),
            Pattern.compile("found multiple function overloads taking arguments from different type categories, when trying to match function\\s+(.+)"),
            Pattern.compile("frame \\S+ offset must not be negative"),
            Pattern.compile("function \\S+\\([^)]*\\) does not exist"),
            Pattern.compile("json field does not contain a numeric field: (.*)"),
            Pattern.compile("missing FROM-clause entry for table \"[^\"]*\""),
            Pattern.compile("null value in column \"[^\"]*\" of relation \"[^\"]*\" violates not-null constraint"),
            Pattern.compile("operator \"[^\"]+\" is not unique"),
            Pattern.compile("operator does not exist:\\s+(.+)"),
            Pattern.compile("operator is not unique: (.*)"),
            Pattern.compile("operator is not unique:\\s+(.+)"),
            Pattern.compile("percentile value ((-?\\d+(.\\d*)?)|inf|-inf|nan) is not between 0 and 1"),
            Pattern.compile("relation \"[^\"]*\" already exists"),
            Pattern.compile("relation \"[^\"]*\" does not exist"),
            Pattern.compile("role \"[^\"]*\" does not exist"),
            Pattern.compile("unit \"[^\"]*\" not recognized for type (.+)"),
            Pattern.compile("unrecognized privilege type:\\s+\"[^\"]*\""),
            Pattern.compile("window \"[^\"]*\" does not exist"),
            Pattern.compile("window \"[^\"]*\" is already defined"),
            Pattern.compile("window function (.+) requires an OVER clause")
    );

    public static List<String> bugErrors() {
        List<String> list = new ArrayList<>();
        if (OxlaBugs.bugOxla3376) {
            list.add("Integer literal error. Value of literal exceeds range.");
        }
        if (OxlaBugs.bugOxla8323) {
            list.add("Invalidated shared object in join processors");
        }
        if (OxlaBugs.bugOxla8330) {
            list.add("syntax error, unexpected RE_CI_MATCH");
        }
        if (OxlaBugs.bugOxla8332) {
            list.add("std::get: wrong index for variant");
        }
        if (OxlaBugs.bugOxla8408) {
            list.add("Logic error in QueryPlanner (if you see this, there is a bug): _Map_base::at");
        }
        return list;
    }

    public static final ExpectedErrors ALL_ERRORS = ExpectedErrors.newErrors()
            .with(OxlaCommon.EXACT_ERRORS)
            .withRegex(OxlaCommon.REGEX_ERRORS)
            .with(OxlaCommon.bugErrors())
            .build();
}
