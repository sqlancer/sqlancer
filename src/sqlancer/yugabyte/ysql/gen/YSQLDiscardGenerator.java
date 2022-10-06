package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable.TableType;

public final class YSQLDiscardGenerator {

    private YSQLDiscardGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("DISCARD ");
        // prevent that DISCARD discards all tables (if they are TEMP tables)
        boolean hasNonTempTables = globalState.getSchema().getDatabaseTables().stream()
                .anyMatch(t -> t.getTableType() == TableType.STANDARD);
        String what;
        if (hasNonTempTables) {
            what = Randomly.fromOptions("ALL", "PLANS", "SEQUENCES", "TEMPORARY", "TEMP");
        } else {
            what = Randomly.fromOptions("PLANS", "SEQUENCES");
        }
        sb.append(what);
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot run inside a transaction block")) {

            @Override
            public boolean couldAffectSchema() {
                return canDiscardTemporaryTables(what);
            }
        };
    }

    private static boolean canDiscardTemporaryTables(String what) {
        return what.contentEquals("TEMPORARY") || what.contentEquals("TEMP") || what.contentEquals("ALL");
    }
}
