package sqlancer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;

public class QueryAdapter extends Query {

    private final String query;
    private final Collection<String> expectedErrors;
    private final boolean couldAffectSchema;

    public QueryAdapter(String query) {
        this(query, new ArrayList<>());
    }

    public QueryAdapter(String query, boolean couldAffectSchema) {
        this(query, new ArrayList<>(), couldAffectSchema);
    }

    public QueryAdapter(String query, Collection<String> expectedErrors) {
        this.query = canonicalizeString(query);
        this.expectedErrors = expectedErrors;
        this.couldAffectSchema = false;
        checkQueryString();
    }

    public QueryAdapter(String query, Collection<String> expectedErrors, boolean couldAffectSchema) {
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
    public boolean execute(GlobalState<?, ?> globalState, String... fills) throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(getQueryString());
            for (int i = 0; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        try {
            s.execute(query);
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e);
            return false;
        }
    }

    public void checkException(Exception e) throws AssertionError {
        boolean isExcluded = false;
        for (String expectedError : expectedErrors) {
            if (e.getMessage().contains(expectedError)) {
                isExcluded = true;
                break;
            }
        }
        if (!isExcluded) {
            throw new AssertionError(query, e);
        }
    }

    @Override
    public ResultSet executeAndGet(GlobalState<?, ?> globalState, String... fills) throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(getQueryString());
            for (int i = 0; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        ResultSet result = null;
        try {
            result = s.executeQuery(query);
            Main.nrSuccessfulActions.addAndGet(1);
            return result;
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
    public Collection<String> getExpectedErrors() {
        return expectedErrors;
    }

}
