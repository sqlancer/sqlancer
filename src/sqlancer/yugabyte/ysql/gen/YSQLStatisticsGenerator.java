package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLStatisticsObject;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public final class YSQLStatisticsGenerator {

    private YSQLStatisticsGenerator() {
    }

    public static SQLQueryAdapter insert(YSQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STATISTICS ");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        YSQLTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView()); // TODO materialized view
        if (randomTable.getColumns().size() < 2) {
            throw new IgnoreMeException();
        }
        sb.append(" ");
        sb.append(getNewStatisticsName(randomTable));
        if (Randomly.getBoolean()) {
            sb.append(" (");
            List<String> statsSubset;
            statsSubset = Randomly.nonEmptySubset("ndistinct", "dependencies", "mcv");
            sb.append(String.join(", ", statsSubset));
            sb.append(")");
        }

        List<YSQLColumn> randomColumns = randomTable.getRandomNonEmptyColumnSubset(
                globalState.getRandomly().getInteger(2, randomTable.getColumns().size()));
        sb.append(" ON ");
        sb.append(randomColumns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot have more than 8 columns in statistics"),
                true);
    }

    public static SQLQueryAdapter remove(YSQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DROP STATISTICS ");
        YSQLTable randomTable = globalState.getSchema().getRandomTable();
        List<YSQLStatisticsObject> statistics = randomTable.getStatistics();
        if (statistics.isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append(Randomly.fromList(statistics).getName());
        return new SQLQueryAdapter(sb.toString(), true);
    }

    private static String getNewStatisticsName(YSQLTable randomTable) {
        List<YSQLStatisticsObject> statistics = randomTable.getStatistics();
        int i = 0;
        while (true) {
            String candidateName = "s" + i;
            if (statistics.stream().noneMatch(stat -> stat.getName().contentEquals(candidateName))) {
                return candidateName;
            }
            i++;
        }
    }

}
