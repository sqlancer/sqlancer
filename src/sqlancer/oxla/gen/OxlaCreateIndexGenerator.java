package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.schema.OxlaColumn;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OxlaCreateIndexGenerator extends OxlaQueryGenerator {
    private static final List<String> errors = List.of(
            "cannot create index in non empty table"
    );
    private static final List<Pattern> regexErrors = List.of(
            Pattern.compile("already have index:\\s+(.*)"),
            Pattern.compile(".*?(?=column)column not supported in index:\\s+(.*)"),
            Pattern.compile("no index column:\\s+(.*)")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // create_index_statement := CREATE INDEX [IF NOT EXISTS] [database_object_name] ON '(' index_column [, ...] ')'
        final var randomTable = globalState.getSchema().getRandomTable();
        final var randomIndexColumns = Randomly.nonEmptySubset(randomTable.getColumns());
        final var queryBuilder = new StringBuilder()
                .append("CREATE INDEX ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(Randomly.getBoolean() ? DBMSCommon.createIndexName(Randomly.smallNumber()) : "")
                .append(" ON ")
                .append(randomTable.getName())
                .append('(')
                .append(randomIndexColumns
                        .stream()
                        .map(this::asIndexColumn)
                        .collect(Collectors.joining(", ")))
                .append(')');
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private String asIndexColumn(OxlaColumn column) {
        // index_column := database_object_name [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]
        final var indexBuilder = new StringBuilder()
                .append(column.getName())
                .append(Randomly.getBoolean() ? (Randomly.getBoolean() ? " ASC" : " DESC") : "")
                .append(Randomly.getBoolean() ? (Randomly.getBoolean() ? " NULLS FIRST" : " NULLS LAST") : "");
        return indexBuilder.toString();
    }
}
