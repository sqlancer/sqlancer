package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.List;
import java.util.regex.Pattern;

public class OxlaCreateTypeGenerator extends OxlaQueryGenerator {
    private static final List<String> errors = List.of(
            "CREATE TYPE statement is not supported.",
            "base type creation not supported"
    );
    private static final List<Pattern> regexErrors = List.of();
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // create_type := CREATE TYPE table_name [ AS '(' [ column_def [, ...] ] ')' ]
        final var queryBuilder = new StringBuilder()
                .append("CREATE TYPE ")
                .append(DBMSCommon.createTableName(Randomly.smallNumber()));
        // AS
        if (Randomly.getBoolean()) {
            queryBuilder
                    .append(" AS ")
                    .append('(')
                    .append(Randomly.getBoolean() ? asColumnList(Randomly.smallNumber()) : "")
                    .append(')');
        }
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    @Override
    public boolean modifiesDatabaseState() {
        return true;
    }

    private String asColumnList(int columnCount) {
        final var columnBuilder = new StringBuilder();
        for (int index = 0; index < columnCount; ++index) {
            // column_def := database_object_name (column_type [ '[]' ... ] | table_name) [ NULL | NOT NULL ]
            // FIXME: Add support for non-basic types (in this case arrays).
            columnBuilder
                    .append(DBMSCommon.createColumnName(Randomly.smallNumber()))
                    .append(' ')
                    .append(Randomly.getBoolean() ? (OxlaDataType.getRandomType()) : DBMSCommon.createTableName(Randomly.smallNumber()));
            if (index + 1 != columnCount) {
                columnBuilder.append(", ");
            }
        }
        return columnBuilder.toString();
    }
}
