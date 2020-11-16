package sqlancer.sqlite3.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.QueryAdapter;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;

// tries to trigger a crash
public class SQLite3Fuzzer implements TestOracle {

    private final SQLite3GlobalState globalState;

    public SQLite3Fuzzer(SQLite3GlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        String s = SQLite3Visitor
                .asString(SQLite3RandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1)) + ";";
        try {
            if (globalState.getDmbsSpecificOptions().executeQuery) {
                globalState.executeStatement(new QueryAdapter(s));
                globalState.getManager().incrementSelectQueryCount();
            }
        } catch (Error e) {

        }
    }

}
