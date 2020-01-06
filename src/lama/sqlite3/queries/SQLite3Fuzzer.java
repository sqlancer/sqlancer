package lama.sqlite3.queries;

import java.sql.SQLException;

import lama.MainOptions;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3Visitor;

// tries to trigger a crash
public class SQLite3Fuzzer {

	public void fuzz(SQLite3GlobalState globalState) throws SQLException {
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
