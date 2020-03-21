package sqlancer.sqlite3.queries;

import java.sql.SQLException;

import sqlancer.MainOptions;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;

// tries to trigger a crash
public class SQLite3Fuzzer implements SQLite3TestGenerator {

	private final SQLite3GlobalState globalState;

	public SQLite3Fuzzer(SQLite3GlobalState globalState) {
		this.globalState = globalState;
	}

	@Override
	public void check() throws SQLException {
		String s = SQLite3Visitor
				.asString(SQLite3RandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1));
		MainOptions options = globalState.getMainOptions();
		try {
			if (options.logEachSelect()) {
				globalState.getLogger().writeCurrent(s);
			}
			globalState.getManager().execute(new QueryAdapter(s));
			globalState.getManager().incrementSelectQueryCount();
		} catch (Error e) {

		}
	}

}
