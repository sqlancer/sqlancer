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

public class OxlaPrivilegeGenerator extends OxlaQueryGenerator {
    enum StatementType {
        GRANT, REVOKE, REVOKE_GRANT;

        @Override
        public String toString() {
            return switch (this) {
                case GRANT -> "GRANT";
                case REVOKE -> "REVOKE";
                case REVOKE_GRANT -> "REVOKE GRANT OPTION FOR";
            };
        }
    }
    enum ClauseType {ALL, DATABASE, EXTERNAL_SOURCE, SCHEMA, TABLE}

    private static final Collection<String> errors = List.of();
    private static final Collection<Pattern> regexErrors = List.of(
            Pattern.compile("syntax error at or near \"[^\"]*\""),
            Pattern.compile("syntax error, unexpected (.+)"),
            Pattern.compile("schema \"[^\"]*\" does not exist")
    );
    public static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // 1. (GRANT | REVOKE | REVOKE GRANT OPTION FOR) privilege_type
        final var statementType = Randomly.fromOptions(StatementType.values());
        final var queryBuilder = new StringBuilder()
                .append(statementType)
                .append(' ')
                .append(getPrivilegeType());

        // 2. ON (ALL database_object_name IN SCHEMA | DATABASE | EXTERNAL SOURCE | SCHEMA | [TABLE])
        final var clauseType = Randomly.fromOptions(ClauseType.values());
        queryBuilder
                .append(" ON ")
                .append(getOnClause(clauseType, globalState))
                .append(' ');

        // 3. (database_object_name | table_name) (FROM | TO) database_object_name
        final var randomly = globalState.getRandomly();
        queryBuilder
                .append(clauseType != ClauseType.TABLE ? randomly.getString(1) : DBMSCommon.createTableName(Randomly.smallNumber()))
                .append(statementType == StatementType.GRANT ? " TO " : " FROM ")
                .append(randomly.getString(1));

        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private String getPrivilegeType() {
        // privilege_type := (ALL PRIVILEGES | (SELECT | INSERT | UPDATE | DELETE | CREATE | CONNECT | USAGE | [, ...]))
        enum PrivilegeType {SELECT, INSERT, UPDATE, CREATE, CONNECT, USAGE}
        return Randomly.getBooleanWithRatherLowProbability()
                ? "ALL PRIVILEGES"
                : Randomly.nonEmptySubsetPotentialDuplicates(Arrays.asList(PrivilegeType.values()))
                .stream()
                .map(PrivilegeType::name)
                .collect(Collectors.joining(", "));
    }

    private String getOnClause(ClauseType type, OxlaGlobalState globalState) {
        return switch (type) {
            case ALL -> String.format("ALL %s IN SCHEMA", globalState.getRandomly().getString(1));
            case DATABASE -> "DATABASE";
            case EXTERNAL_SOURCE -> "EXTERNAL SOURCE";
            case SCHEMA -> "SCHEMA";
            case TABLE -> Randomly.getBoolean() ? "TABLE" : "";
        };
    }
}
