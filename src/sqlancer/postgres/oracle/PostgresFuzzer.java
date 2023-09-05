package sqlancer.postgres.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.gen.PostgresRandomQueryGenerator;

public class PostgresFuzzer implements TestOracle<PostgresGlobalState> {

    private final PostgresGlobalState globalState;

    public PostgresFuzzer(PostgresGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        String s = PostgresVisitor.asString(
                PostgresRandomQueryGenerator.createRandomQuery(Randomly.smallNumber() + 1, globalState)) + ';';
        try {
            globalState.executeStatement(new SQLQueryAdapter(s));
            globalState.getManager().incrementSelectQueryCount();
        } catch (Error e) {

        }
    }

}
