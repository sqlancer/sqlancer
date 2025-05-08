package sqlancer.oxla;

import java.util.List;
import java.util.regex.Pattern;

public class OxlaCommon {
    private OxlaCommon() {
    }

    public static final List<String> SYNTAX_ERRORS = List.of(
            "invalid input syntax for type"
    );
    public static final List<Pattern> SYNTAX_REGEX_ERRORS = List.of(
            Pattern.compile("operator \"[^\"]+\" is not unique"),
            Pattern.compile("operator is not unique: (.*)")
    );
    public static final List<String> JOIN_ERRORS = List.of(
            "invalid JOIN ON clause condition. Only equi join is supported"
    );
    public static final List<String> GROUP_BY_ERRORS = List.of(
            "non-integer constant in GROUP BY"
    );
    public static final List<Pattern> GROUP_BY_REGEX_ERRORS = List.of(
            Pattern.compile("GROUP BY position (\\d+) is not in select list")
    );
    public static final List<String> ORDER_BY_ERRORS = List.of(
            "non-integer constant in ORDER BY"
    );
    public static final List<Pattern> ORDER_BY_REGEX_ERRORS = List.of(
            Pattern.compile("ORDER BY position (\\d+) is not in select list")
    );
}
