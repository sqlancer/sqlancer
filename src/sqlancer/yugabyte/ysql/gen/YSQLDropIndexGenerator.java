package sqlancer.yugabyte.ysql.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLIndex;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public final class YSQLDropIndexGenerator {

    private YSQLDropIndexGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        YSQLTable randomTable = globalState.getSchema().getRandomTable();
        List<YSQLIndex> indexes = randomTable.getIndexes();
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        if (Randomly.getBoolean() || indexes.isEmpty()) {
            sb.append("IF EXISTS ");
            if (indexes.isEmpty() || Randomly.getBoolean()) {
                sb.append(DBMSCommon.createIndexName(randomTable.getName(), Randomly.smallNumber()));
            } else {
                sb.append(Randomly.fromList(indexes).getIndexName());
            }
        } else {
            sb.append(Randomly.fromList(indexes).getIndexName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("cannot drop desired object(s) because other objects depend on them",
                        "cannot drop index", "does not exist"),
                true);
    }

}
