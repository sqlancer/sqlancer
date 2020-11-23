package sqlancer.common.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;

public class SQLQueryResultCheckAdapter<G extends SQLGlobalState<?, ?>> extends SQLQueryAdapter<G> {

    private final Consumer<ResultSet> rsChecker;

    public SQLQueryResultCheckAdapter(String query, Consumer<ResultSet> rsChecker) {
        super(query);
        this.rsChecker = rsChecker;
    }

    @Override
    public boolean execute(G globalState, String... fills) throws SQLException {
        try (Statement s = globalState.getConnection().createStatement()) {
            ResultSet rs = s.executeQuery(getQueryString());
            rsChecker.accept(rs);
            return true;
        } catch (Exception e) {
            checkException(e);
            return false;
        }
    }

}
