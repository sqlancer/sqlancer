package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.List;
import java.util.regex.Pattern;

public class OxlaDropTypeGenerator extends OxlaQueryGenerator {
    private static final List<String> errors = List.of(
            "DROP TYPE statement is not supported"
    );
    private static final List<Pattern> regexErrors = List.of();
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // drop_type := DROP TYPE [IF EXISTS] database_object_name [ CASCADE | RESTRICT ]
        final var queryBuilder = new StringBuilder()
                .append("DROP TYPE ")
                .append(Randomly.getBoolean() ? "IF EXISTS " : "")
                .append(DBMSCommon.createSchemaName(Randomly.smallNumber()))
                .append(Randomly.getBoolean() ? (Randomly.getBoolean() ? " CASCADE" : " RESTRICT") : "");
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    @Override
    public boolean modifiesDatabaseState() {
        return true;
    }
}
