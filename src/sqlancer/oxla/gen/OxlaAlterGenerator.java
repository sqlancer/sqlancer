package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OxlaAlterGenerator extends OxlaQueryGenerator {
    private static final Collection<String> errors = List.of(
            "password cannot be empty",
            "conflicting or redundant options"
    );
    private static final Collection<Pattern> regexErrors = List.of(
            Pattern.compile("schema \"[^\"]*\" does not exist"),
            Pattern.compile("role \"[^\"]*\" already exist"),
            Pattern.compile("syntax error, unexpected (.+)")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // role_option          := (LOGIN) | (SUPERUSER | NOSUPERUSER) | (PASSWORD <string>)
        // alter_owner_table    := ALTER TABLE table_name OWNER TO database_object_name
        // alter_owner_schema   := ALTER SCHEMA database_object_name OWNER TO database_object_name
        // alter_owner_database := ALTER DATABASE database_object_name OWNER TO database_object_name
        // alter_user           := ALTER USER database_object_name [WITH] [role_option [...]]
        // alter_role           := ALTER ROLE database_object_name [WITH] [role_option [...]]
        // alter_statement      := alter_role | alter_database_owner | alter_schema_owner | alter_table_owner
        enum Rule {
            ROLE, USER, OWNER_DATABASE, OWNER_SCHEMA, OWNER_TABLE;

            @Override
            public String toString() {
                return switch (this) {
                    case ROLE -> "ROLE";
                    case USER -> "USER";
                    case OWNER_DATABASE -> "DATABASE";
                    case OWNER_SCHEMA -> "SCHEMA";
                    case OWNER_TABLE -> "TABLE";
                };
            }
        }
        final var type = Randomly.fromOptions(Rule.values());
        final var randomly = globalState.getRandomly();
        final var options = Randomly.nonEmptySubsetPotentialDuplicates(Arrays.asList(OxlaCreateRoleGenerator.RoleOption.values()));
        final var queryBuilder = new StringBuilder()
                .append("ALTER ")
                .append(type)
                .append(' ');
        if (type == Rule.USER || type == Rule.ROLE) {
            queryBuilder
                    .append(randomly.getString(1))
                    .append(Randomly.getBoolean() ? " WITH " : "")
                    .append(options
                            .stream()
                            .map(o -> OxlaCreateRoleGenerator.asRoleOption(globalState, o))
                            .collect(Collectors.joining(" ")));
        } else {
            queryBuilder
                    .append(type != Rule.OWNER_TABLE ? randomly.getString(1) : DBMSCommon.createTableName(Randomly.smallNumber()))
                    .append(" OWNER TO ")
                    .append(randomly.getString(1));
        }

        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }
}
