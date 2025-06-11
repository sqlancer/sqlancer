package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.OxlaToStringVisitor;

import java.util.List;
import java.util.regex.Pattern;

public class OxlaDeleteFromGenerator extends OxlaQueryGenerator {
    private static final List<String> errors = List.of(
            "ONLY clause in DELETE statement is not supported"
    );
    private static final List<Pattern> regexErrors = List.of(
            Pattern.compile("other modification of table \"[^\"]+\" is in progress")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    public OxlaDeleteFromGenerator() {
    }

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int ignored) {
        final var tableName = Randomly.fromList(globalState.getSchema().getDatabaseTables()).getName();
        return new SQLQueryAdapter(Randomly.getBooleanWithRatherLowProbability()
                ? getTruncateQuery(tableName) : getDeleteQuery(globalState, tableName), expectedErrors);
    }

    private String getDeleteQuery(OxlaGlobalState globalState, String tableName) {
        // delete_statement := DELETE FROM [ ONLY ] table_name [ AS [ alias ] ] [ WHERE condition ]
        StringBuilder queryBuilder = new StringBuilder()
                .append("DELETE FROM ")
                .append(Randomly.getBoolean() ? "ONLY " : "")
                .append(tableName);

        if (Randomly.getBoolean()) {
            queryBuilder.append(" AS ")
                    .append(tableName)
                    .append('_')
                    .append("_aliased")
                    .append(' ');
        }

        if (Randomly.getBoolean()) {
            queryBuilder
                    .append(" WHERE ")
                    .append(OxlaToStringVisitor
                            .asString(new OxlaExpressionGenerator(globalState).generatePredicate()));
        }
        return queryBuilder.toString();
    }

    private String getTruncateQuery(String tableName) {
        // truncate_statement := TRUNCATE [TABLE] table_name
        return new StringBuilder()
                .append("TRUNCATE ")
                .append(Randomly.getBoolean() ? "TABLE " : "")
                .append(tableName)
                .toString();
    }
}
