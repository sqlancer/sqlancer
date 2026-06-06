package sqlancer.postgres.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;

public class PostgresTableSpaceGenerator {

    private final ExpectedErrors errors = new ExpectedErrors();
    private final PostgresGlobalState globalState;

    public PostgresTableSpaceGenerator(PostgresGlobalState globalState) {
        this.globalState = globalState;
        errors.addRegexString("ERROR: (?:tablespace )?directory \".*[\\\\/]tablespace\\d+\" does not exist");
        errors.add("ERROR: already exists");
        errors.add("ERROR: is not empty");
        errors.add("ERROR: cannot be created because system does not support tablespaces");
    }

    public static SQLQueryAdapter generate(PostgresGlobalState globalState) {
        // Skip tablespace generation if the option is disabled
        PostgresOptions options = globalState.getDbmsSpecificOptions();
        if (!options.isTestTablespaces()) {
            return null;
        }
        return new PostgresTableSpaceGenerator(globalState).generateTableSpace();
    }

    private SQLQueryAdapter generateTableSpace() {
        StringBuilder sb = new StringBuilder();
        int tableSpaceNum = globalState.getRandomly().getInteger(1, Integer.MAX_VALUE);

        // CREATE TABLESPACE syntax
        sb.append("CREATE TABLESPACE ");
        sb.append("tablespace");
        sb.append(tableSpaceNum);
        sb.append(" LOCATION '");

        // Get the validated base path from options and append the tablespace number
        PostgresOptions options = globalState.getDbmsSpecificOptions();
        String path = options.getTablespacePath() + tableSpaceNum;

        // Convert backslashes to forward slashes for PostgreSQL
        path = path.replace('\\', '/');

        // Escape single quotes in the path
        path = path.replace("'", "''");

        sb.append(path);
        sb.append("'");

        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
