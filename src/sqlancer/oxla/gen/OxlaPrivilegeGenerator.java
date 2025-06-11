package sqlancer.oxla.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public class OxlaPrivilegeGenerator extends OxlaQueryGenerator {
    private static final Collection<String> errors = List.of();
    private static final Collection<Pattern> regexErrors = List.of();
    public static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // privilege_type := (ALL PRIVILEGES | (SELECT | INSERT | UPDATE | DELETE | CREATE | CONNECT | USAGE | [, ...]))
        // privilege_statement := GRANT privilege_type ON table_name TO database_object_name
        //                      | REVOKE privilege_type ON table_name FROM database_object_name
        //                      | REVOKE GRANT OPTION privilege_type ON table_name FROM database_object_name
        //                      | GRANT privilege_type ON TABLE table_name TO database_object_name
        //                      | REVOKE privilege_type ONE TABLE table_name FROM database_object_name
        //                      | REVOKE GRANT OPTION FOR privilege_type ON TABLE table_name FROM database_object
        //                      | GRANT privilege_type ON ALL database_object_name IN SCHEMA database_objecT_name TO 
        final var queryBuilder = new StringBuilder();
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }
}
