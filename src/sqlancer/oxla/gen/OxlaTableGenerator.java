package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public class OxlaTableGenerator {
    private static final Collection<String> errors = List.of();
    private static final Collection<Pattern> regexErrors = List.of(
            Pattern.compile("relation \"[^\"]*\" already exists"),
            Pattern.compile("column \"[^\"]*\" has unsupported type")
    );

    private OxlaTableGenerator() {
    }

    public static SQLQueryAdapter generate(String tableName, OxlaGlobalState globalState) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE");
        if (Randomly.getBoolean()) {
            queryBuilder.append(" IF NOT EXISTS");
        }
        queryBuilder.append(' ');
        queryBuilder.append(tableName);
        queryBuilder.append('(');

        final int columnCount = Randomly.smallNumber() + 1;
        for (int index = 0; index < columnCount; ++index) {
            String columnName = DBMSCommon.createColumnName(index);
            queryBuilder.append(columnName);
            queryBuilder.append(' ');
            queryBuilder.append(OxlaDataType.toString(OxlaDataType.getRandomType()));
            queryBuilder.append(' ');
            queryBuilder.append(Randomly.fromOptions("NOT NULL", "NULL"));

            if (index + 1 != columnCount) {
                queryBuilder.append(',');
            }
        }
        queryBuilder.append(')');
        return new SQLQueryAdapter(queryBuilder.toString(), new ExpectedErrors(errors, regexErrors), true);
    }
}
