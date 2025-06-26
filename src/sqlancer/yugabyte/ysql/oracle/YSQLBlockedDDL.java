package sqlancer.yugabyte.ysql.oracle;

import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLProvider;
import sqlancer.yugabyte.ysql.gen.YSQLTableGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class YSQLBlockedDDL implements TestOracle<YSQLGlobalState> {
    protected final YSQLGlobalState state;

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final Main.StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;

    private final List<String> tempTables = new ArrayList<>();

    private final List<YSQLProvider.Action> dmlActions = Arrays.asList(YSQLProvider.Action.INSERT,
            YSQLProvider.Action.UPDATE, YSQLProvider.Action.DELETE);
    private final List<YSQLProvider.Action> catalogActions = Arrays.asList(YSQLProvider.Action.TRUNCATE, YSQLProvider.Action.CREATE_VIEW,
            YSQLProvider.Action.REFRESH_VIEW, YSQLProvider.Action.CREATE_SEQUENCE, YSQLProvider.Action.ALTER_TABLE,
            YSQLProvider.Action.DROP_INDEX, YSQLProvider.Action.COMMENT_ON);
    private final List<YSQLProvider.Action> allowedDdls = Arrays.asList(YSQLProvider.Action.RESET_ROLE,
            YSQLProvider.Action.VACUUM, YSQLProvider.Action.DISCARD, YSQLProvider.Action.RESET, YSQLProvider.Action.SET_CONSTRAINTS);

    public YSQLBlockedDDL(YSQLGlobalState globalState) {
        globalState.getDbmsSpecificOptions().createDatabases = false;

        this.state = globalState;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonFetchErrors(errors);
        YSQLErrors.addTransactionErrors(errors);
    }

    private YSQLProvider.Action getRandomAction(List<YSQLProvider.Action> actions) {
        return actions.get(state.getRandomly().getInteger(0, actions.size()));
    }

    @Override
    public void check() throws Exception {
        // create table or evaluate catalog test
        int seed = state.getRandomly().getInteger(1, 100);
        if (seed > 50) {
            YSQLProvider.Action randomAction;
            if (Randomly.getBoolean()) {
                randomAction = getRandomAction(dmlActions);
            } else {
                randomAction = getRandomAction(allowedDdls);
            }

            ExpectedErrors errors = randomAction.getQuery(state).getExpectedErrors();

            errors.add("ON CONFLICT");
            errors.add("ON COMMIT");
            errors.add("heap tid from index");

            SQLQueryAdapter newQuery = new SQLQueryAdapter(randomAction.getQuery(state).getQueryString(), errors);
            state.executeStatement(newQuery);
        } else {
            SQLQueryAdapter query;
            if (seed > 35) {
                String tableName = DBMSCommon.createTableName(state.getSchema().getDatabaseTables().size());
                query = YSQLTableGenerator.generate(tableName, true, state);
                ;
            } else {
                query = getRandomAction(catalogActions).getQuery(state);
            }

            ExpectedErrors errors = new ExpectedErrors();
            errors.add("YSQL DDLs, and catalog modifications are not allowed during a major YSQL upgrade");

            errors.add("not a table");
            errors.add("invalid input syntax for type");
            errors.add("must not be");
            errors.add("must be");
            errors.add("cannot");

            errors.add("unrecognized parameter");
            errors.add("pseudo-type unknown");
            errors.add("not yet supported");
            errors.add("not exist");
            errors.add("does not exist");
            errors.add("not allowed");
            errors.add("already exists");
            errors.add("no unique constraint matching given keys");
            errors.add("conflicting NULL/NOT NULL declarations");
            errors.add("materialized views must not use temporary tables or views");
            errors.add("could not create unique index");
            errors.add("non-integer constant in");
            errors.add("must appear in select list");
            errors.add("no unique or exclusion");
            errors.add("violate");
            errors.add("null values");
            errors.add("malformed");
            errors.add("must appear");
            errors.add("must match");
            errors.add("would not be");
            errors.add("out of range");
            errors.add("not a valid");
            errors.add("invalid regular expression");
            errors.add("unsupported");
            errors.add("operator is not unique");
            errors.add("concurrent update");
            errors.add("not in select list");

            boolean mightBeAllowed = query.getQueryString().contains("TEMP");

            SQLQueryAdapter newQuery = new SQLQueryAdapter(query.getQueryString(), errors);
            if (state.executeStatement(newQuery)) {
                if (!mightBeAllowed) {
                    boolean tableFound = false;
                    if (query.getQueryString().contains("ALTER TABLE") || query.getQueryString().contains("ADD CONSTRAINT") || query.getQueryString().contains("TRUNCATE")) {
                        for (String tempTable : tempTables) {
                            if (query.getQueryString().contains(tempTable)) {
                                tableFound = true;
                                break;
                            }
                        }
                        if (!tableFound) {
                            throw new AssertionError(query + String.format("%s", !state.executeStatement(newQuery)));
                        }
                    }

                    if (!tableFound && !query.getQueryString().contains("DROP INDEX") && !query.getQueryString().contains("VIEW") && !query.getQueryString().contains("COMMENT")) {
                        // index "i4" does not exist, skipping - not an error
                        throw new AssertionError(query + String.format("%s", !state.executeStatement(newQuery)));
                    }
                } else {
                    Pattern pattern = Pattern.compile("\\b(t[0-9]*|seq)\\b", Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(query.getQueryString());
                    if (query.getQueryString().contains("TEMP") && query.getQueryString().contains("CREATE")) {
                        while (matcher.find()) {
                            tempTables.add(matcher.group());
                        }
                    }
                    if (query.getQueryString().contains("DROP")) {
                        while (matcher.find()) {
                            tempTables.remove(matcher.group());
                        }
                    }
                }
            }
        }

        state.getManager().incrementSelectQueryCount();
    }
}
