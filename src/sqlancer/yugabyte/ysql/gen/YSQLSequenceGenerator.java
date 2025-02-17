package sqlancer.yugabyte.ysql.gen;

import sqlancer.common.gen.AbstractSequenceGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLSequenceGenerator extends AbstractSequenceGenerator<YSQLGlobalState> {

    private YSQLSequenceGenerator() {
    }

    public static SQLQueryAdapter createSequence(YSQLGlobalState globalState) {
        return AbstractSequenceGenerator.createSequence(globalState);
    }
}
