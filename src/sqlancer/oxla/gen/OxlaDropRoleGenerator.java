package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.List;
import java.util.regex.Pattern;

public class OxlaDropRoleGenerator extends OxlaQueryGenerator {
    private static final List<String> errors = List.of();
    private static final List<Pattern> regexErrors = List.of(
            Pattern.compile("role \"[^\"]*\" does not exist")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // drop_role := DROP ROLE [IF EXISTS] database_object_name
        final var isRole = Randomly.getBoolean();
        final var index = Randomly.smallNumber();
        final var queryBuilder = new StringBuilder()
                .append("DROP ")
                .append(isRole ? "ROLE " : "USER ")
                .append(Randomly.getBoolean() ? "IF EXISTS " : "")
                .append(isRole ? DBMSCommon.createRoleName(index) : DBMSCommon.createUserName(index));
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    @Override
    public boolean modifiesDatabaseState() {
        return true;
    }
}
