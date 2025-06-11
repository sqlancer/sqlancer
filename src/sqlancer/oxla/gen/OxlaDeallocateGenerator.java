package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public class OxlaDeallocateGenerator extends OxlaQueryGenerator {
    private static final Collection<String> errors = List.of();
    private static final Collection<Pattern> regexErrors = List.of();
    public static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // deallocate_statement := DEALLOCATE [PREPARE] (database_object_name | ALL)
        final var queryBuilder = new StringBuilder()
                .append("DEALLOCATE ")
                .append(Randomly.getBoolean() ? "PREPARE " : "")
                .append(Randomly.getBooleanWithRatherLowProbability() ? "ALL" : globalState.getRandomly().getString());
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }
}
