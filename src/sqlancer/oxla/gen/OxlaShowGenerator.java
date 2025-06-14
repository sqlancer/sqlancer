package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.List;
import java.util.regex.Pattern;

public class OxlaShowGenerator extends OxlaQueryGenerator {
    enum ShowOption {
        DATABASES, TABLES, NODES, COLUMNS, RUNTIME;

        public String asString(OxlaGlobalState globalState) {
            final var randomly = globalState.getRandomly();
            return switch (this) {
                case DATABASES -> "DATABASES";
                case TABLES -> "TABLES";
                case NODES -> "NODES";
                case COLUMNS ->
                        Randomly.getBoolean() ? "COLUMNS" : String.format("COLUMNS %s", globalState.getSchema().getRandomTable().getName());
                case RUNTIME ->
                        Randomly.getBoolean() ? randomly.getString(1) : String.format("%s.%s", randomly.getString(1), randomly.getString(1));
            };
        }
    }

    private static final List<String> errors = List.of(
            "Expected table name after COLUMNS"
    );
    private static final List<Pattern> regexErrors = List.of(
            Pattern.compile("SYSTEM table: \\S+ does not exist"),
            Pattern.compile("unrecognized configuration parameter \"[^\"]*\""),
            Pattern.compile("syntax error, unexpected (.+)")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // show_statement := DESCRIBE DATABASE [table_name]
        //                 | DESCRIBE TABLE [table_name]
        //                 | SHOW (DATABASES | TABLES | NODES | COLUMNS | database_object_name ['.' database_object_name])
        enum Rule {DESCRIBE_DATABASE, DESCRIBE_TABLE, SHOW}
        final var queryBuilder = new StringBuilder();
        final var tableName = globalState.getSchema().getRandomTable().getName();
        switch (Randomly.fromOptions(Rule.values())) {
            case DESCRIBE_DATABASE -> queryBuilder
                    .append("DESCRIBE DATABASE")
                    .append(Randomly.getBoolean() ? String.format(" %s", tableName) : "");

            case DESCRIBE_TABLE -> queryBuilder
                    .append("DESCRIBE TABLE ")
                    .append(tableName);
            case SHOW -> queryBuilder
                    .append("SHOW ")
                    .append(Randomly.fromOptions(ShowOption.values()).asString(globalState));
        }
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }
}
