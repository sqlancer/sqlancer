package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.schema.OxlaTable;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public class OxlaDropTableGenerator extends OxlaQueryGenerator {
    private static final Collection<String> errors = List.of(
            "only DROP SCHEMA and TABLE are currently supported"
    );
    private static final Collection<Pattern> regexErrors = List.of(
            Pattern.compile("relation \"[^\"]*\" does not exist")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    public OxlaDropTableGenerator() {
    }

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int ignored) {
        final OxlaTable randomTable = globalState.getSchema().getRandomTable();
        final StringBuilder queryBuilder = new StringBuilder()
                .append("DROP ")
                .append(Randomly.getBoolean() ? "TABLE " : "VIEW ")
                .append(Randomly.getBoolean() ? "IF EXISTS " : "")
                .append(randomTable.getName());
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    @Override
    public boolean modifiesDatabaseState() {
        return true;
    }
}
