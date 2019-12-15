package lama;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class QueryResultCheckAdapter extends QueryAdapter {

	private final Consumer<ResultSet> rsChecker;

	public QueryResultCheckAdapter(String query, Consumer<ResultSet> rsChecker) {
		super(query);
		this.rsChecker = rsChecker;
	}
	
	@Override
	public void execute(Connection con) throws SQLException {
		try (Statement s = con.createStatement()) {
			ResultSet rs = s.executeQuery(getQueryString());
			rsChecker.accept(rs);
		} catch (Exception e) {
			checkException(e);
		}
	}

}
