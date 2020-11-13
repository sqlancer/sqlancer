package sqlancer.common.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import sqlancer.GlobalState;
import sqlancer.Main;

public class QueryAdapter extends Query {

    private final String query;
    private final ExpectedErrors expectedErrors;
    private final boolean couldAffectSchema;

    public QueryAdapter(String query) {
        this(query, new ExpectedErrors());
    }

    public QueryAdapter(String query, boolean couldAffectSchema) {
        this(query, new ExpectedErrors(), couldAffectSchema);
    }

    public QueryAdapter(String query, ExpectedErrors expectedErrors) {
        this(query, expectedErrors, false);
    }

    public QueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        this.query = canonicalizeString(query);
        this.expectedErrors = expectedErrors;
        this.couldAffectSchema = couldAffectSchema;
        checkQueryString();
    }

    private String canonicalizeString(String s) {
        if (s.endsWith(";")) {
            return s;
        } else if (!s.contains("--")) {
            return s + ";";
        } else {
            // query contains a comment
            return s;
        }
    }

    private void checkQueryString() {
        if (query.contains("CREATE TABLE") && !couldAffectSchema) {
            throw new AssertionError("CREATE TABLE statements should set couldAffectSchema to true");
        }
    }

    @Override
    public String getQueryString() {
        return query;
    }

    @Override
    public String getUnterminatedQueryString() {
        String result;
        if (query.endsWith(";")) {
            result = query.substring(0, query.length() - 1);
        } else {
            result = query;
        }
        assert !result.endsWith(";");
        return result;
    }

    @Override
    public boolean execute(GlobalState<?, ?> globalState, String... fills) throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        try {
            if (fills.length > 0) {
                ((PreparedStatement) s).execute();
            } else {
                s.execute(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e);
            return false;
        }
    }

    public void checkException(Exception e) throws AssertionError {
        if (!expectedErrors.errorIsExpected(e.getMessage())) {
            throw new AssertionError(query, e);
        }
    }

    @Override
    public SQLancerResultSet executeAndGet(GlobalState<?, ?> globalState, String... fills) throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        ResultSet result = null;
        try {
            if (fills.length > 0) {
                result = ((PreparedStatement) s).executeQuery();
            } else {
                result = s.executeQuery(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            if (result == null) {
                return null;
            }
            return new SQLancerResultSet(result);
        } catch (Exception e) {
            s.close();
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e);
        }
        return null;
    }

    @Override
    public boolean couldAffectSchema() {
        return couldAffectSchema;
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return expectedErrors;
    }

    @Override
    public String getLogString() {
        return getQueryString();
    }
}
