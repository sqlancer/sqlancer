package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public class OxlaDiscardGenerator extends OxlaQueryGenerator {
    private static final Collection<String> errors = List.of();
    private static final Collection<Pattern> regexErrors = List.of();
    public static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // discard_statement := DISCARD (ALL | PLANS | SEQUENCES | TEMPORARY | TEMP)
        enum Rule {ALL, PLANS, SEQUENCES, TEMPORARY, TEMP}
        return new SQLQueryAdapter(
                new StringBuilder()
                        .append("DISCARD ")
                        .append(Randomly.fromOptions(Rule.values()).name()).toString(),
                expectedErrors);
    }
}
