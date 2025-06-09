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
        errors.addRegexString("ERROR: (?:tablespace )?directory \".*[\\\\/]tablespace[1-5]\" does not exist");
        errors.add("ERROR: must be a directory");
        errors.add("ERROR: permission denied");
        errors.add("ERROR: already exists");
        errors.add("ERROR: is not empty");
        errors.add("ERROR: cannot be created because system does not support tablespaces");
    }

    public static SQLQueryAdapter generate(PostgresGlobalState globalState) {
        // Skip tablespace generation if the option is disabled
        PostgresOptions options = globalState.getDbmsSpecificOptions();
        if (!options.testTablespaces) {
            return null;
        }
        return new PostgresTableSpaceGenerator(globalState).generateTableSpace();
    }

    private SQLQueryAdapter generateTableSpace() {
        StringBuilder sb = new StringBuilder();
        int tableSpaceNum = globalState.getRandomly().getInteger(1, 5);

        // CREATE TABLESPACE syntax
        sb.append("CREATE TABLESPACE ");
        sb.append("tablespace");
        sb.append(tableSpaceNum);
        sb.append(" LOCATION '");

        // Get the base path from options and append the tablespace number
        PostgresOptions options = globalState.getDbmsSpecificOptions();
        String path = options.tablespacePath + tableSpaceNum;

        // Convert backslashes to forward slashes for PostgreSQL
        path = path.replace('\\', '/');

        // Escape single quotes in the path
        path = path.replace("'", "''");

        sb.append(path);
        sb.append("'");

        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
