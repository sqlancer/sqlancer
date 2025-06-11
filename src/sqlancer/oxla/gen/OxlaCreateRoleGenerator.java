package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OxlaCreateRoleGenerator extends OxlaQueryGenerator {
    public enum RoleOption {LOGIN, USERTYPE, PASSWORD}

    private static final List<String> errors = List.of(
            "conflicting or redundant options",
            "password must be provided",
            "password cannot be empty"
    );
    private static final List<Pattern> regexErrors = List.of();
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // role_option := (LOGIN) | (SUPERUSER | NOSUPERUSER) | (PASSWORD <string>)
        // create_role := CREATE (ROLE | USER) database_object_name [WITH] [role_option [...]]
        final var isRole = Randomly.getBoolean();
        final var index = Randomly.smallNumber();
        final var options = Randomly.nonEmptySubsetPotentialDuplicates(Arrays.asList(RoleOption.values()));
        final var queryBuilder = new StringBuilder()
                .append("CREATE ")
                .append(isRole ? "ROLE " : "USER ")
                .append(isRole ? DBMSCommon.createRoleName(index) : DBMSCommon.createUserName(index))
                .append(Randomly.getBoolean() ? " WITH " : "")
                .append(options
                        .stream()
                        .map(o -> asRoleOption(globalState, o))
                        .collect(Collectors.joining(" ")));
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    @Override
    public boolean modifiesDatabaseState() {
        return true;
    }

    public static String asRoleOption(OxlaGlobalState globalState, RoleOption option) {
        return switch (option) {
            case LOGIN -> "LOGIN";
            case USERTYPE -> Randomly.getBoolean() ? "SUPERUSER" : "NOSUPERUSER";
            case PASSWORD -> String.format("PASSWORD '%s'", globalState.getRandomly().getString());
        };
    }
}
