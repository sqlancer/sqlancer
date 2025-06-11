package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.OxlaToStringVisitor;
import sqlancer.oxla.schema.OxlaTable;

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
        // delete_statement := DELETE FROM [ ONLY ] table_name [ AS [ alias ] ] [ WHERE condition ]
        final OxlaTable table = Randomly.fromList(globalState.getSchema().getDatabaseTables());
        StringBuilder queryBuilder = new StringBuilder()
                .append("DELETE FROM ")
                .append(Randomly.getBoolean() ? "ONLY " : "")
                .append(table.getName());

        if (Randomly.getBoolean()) {
            queryBuilder.append(" AS ")
                    .append(table.getName())
                    .append('_')
                    .append("_aliased")
                    .append(' ');
        }

        if (Randomly.getBoolean()) {
            queryBuilder.append(" WHERE ").append(OxlaToStringVisitor.asString(new OxlaExpressionGenerator(globalState).generatePredicate()));
        }
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }
}
