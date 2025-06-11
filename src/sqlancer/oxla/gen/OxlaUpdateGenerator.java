package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaTable;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OxlaUpdateGenerator extends OxlaQueryGenerator {
    private static final List<String> errors = List.of(
            "FROM clause in UPDATE statement is not supported.",
            "ONLY clause in UPDATE statement is not supported."
    );
    private static final List<Pattern> regexErrors = List.of(
            Pattern.compile("other modification of table \"[^\"]+\" is in progress")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    public OxlaUpdateGenerator() {
    }

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int ignored) {
        // update_statement := UPDATE [ ONLY ] table_name [ [ AS ] alias ] SET { column_name = expression [, ...] } [ FROM [, ...] ] [ WHERE condition ]
        final OxlaTable table = Randomly.fromList(globalState.getSchema().getDatabaseTables());

        StringBuilder queryBuilder = new StringBuilder()
                .append("UPDATE ")
                .append(Randomly.getBoolean() ? "ONLY " : "")
                .append(table.getName());

        // ALIAS
        if (Randomly.getBoolean()) {
            queryBuilder
                    .append(" AS ")
                    .append(table.getName())
                    .append("_aliased")
                    .append(' ');
        }

        // SET
        queryBuilder.append(" SET ");
        final List<OxlaColumn> columns = table.getRandomNonEmptyColumnSubset();
        final var generator = new OxlaExpressionGenerator(globalState);
        for (int index = 0; index < columns.size(); ++index) {
            final OxlaColumn column = columns.get(index);
            queryBuilder
                    .append(column.getName())
                    .append("=")
                    .append(generator.generateExpression(column.getType()));
            if (index + 1 != columns.size()) {
                queryBuilder.append(", ");
            }
        }

        // FROM
        if (Randomly.getBoolean()) {
            final List<OxlaTable> randomTables = Randomly.nonEmptySubset(globalState.getSchema().getDatabaseTables());
            queryBuilder
                    .append(" FROM ")
                    .append(randomTables
                            .stream()
                            .map(OxlaTable::getName)
                            .collect(Collectors.joining(", ")));
        }

        // WHERE
        if (Randomly.getBoolean()) {
            queryBuilder.append(" WHERE ").append(generator.generatePredicate());
        }

        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }
}
