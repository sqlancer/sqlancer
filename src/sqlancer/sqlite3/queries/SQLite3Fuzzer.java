package sqlancer.sqlite3.queries;

import java.sql.SQLException;

import sqlancer.MainOptions;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;

// tries to trigger a crash
public class SQLite3Fuzzer implements TestOracle {

	private final SQLite3GlobalState globalState;

	public SQLite3Fuzzer(SQLite3GlobalState globalState) {
		this.globalState = globalState;
	}

	@Override
	public void check() throws SQLException {
		String s = SQLite3Visitor
				.asString(SQLite3RandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1)) + ";";
		MainOptions options = globalState.getOptions();
		try {
			if (options.logEachSelect()) {
				globalState.getLogger().writeCurrent(s);
			}
			if (globalState.getDmbsSpecificOptions().printStatements) {
				System.out.println(s);
			}
			if (globalState.getDmbsSpecificOptions().executeQuery) {
				globalState.getManager().execute(new QueryAdapter(s));
				if (globalState.getDmbsSpecificOptions().executeStatementsAndPrintSuccessfulOnes) {
					System.out.println(s);
				}
				globalState.getManager().incrementSelectQueryCount();
			}
		} catch (Error e) {

		}
	}

}
