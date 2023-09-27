package sqlancer.mysql.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.gen.MySQLRandomQuerySynthesizer;

public class MySQLFuzzer implements TestOracle<MySQLGlobalState> {

    private final MySQLGlobalState globalState;

    public MySQLFuzzer(MySQLGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        String s = MySQLVisitor.asString(MySQLRandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1))
                + ';';
        try {
            globalState.executeStatement(new SQLQueryAdapter(s));
            globalState.getManager().incrementSelectQueryCount();
        } catch (Error e) {

        }
    }

}
