package sqlancer.materialize.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeStatisticsObject;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;

public final class MaterializeStatisticsGenerator {

    private MaterializeStatisticsGenerator() {
    }

    public static SQLQueryAdapter insert(MaterializeGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STATISTICS ");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        MaterializeTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView()); // TODO materialized view
        if (randomTable.getColumns().size() < 2) {
            throw new IgnoreMeException();
        }
        sb.append(" ");
        sb.append(getNewStatisticsName(randomTable));
        if (Randomly.getBoolean()) {
            sb.append(" (");
            List<String> statsSubset;
            statsSubset = Randomly.nonEmptySubset("ndistinct", "dependencies", "mcv");
            sb.append(statsSubset.stream().collect(Collectors.joining(", ")));
            sb.append(")");
        }

        List<MaterializeColumn> randomColumns = randomTable.getRandomNonEmptyColumnSubset(
                globalState.getRandomly().getInteger(2, randomTable.getColumns().size()));
        sb.append(" ON ");
        sb.append(randomColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot have more than 8 columns in statistics"),
                true);
    }

    public static SQLQueryAdapter remove(MaterializeGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DROP STATISTICS ");
        MaterializeTable randomTable = globalState.getSchema().getRandomTable();
        List<MaterializeStatisticsObject> statistics = randomTable.getStatistics();
        if (statistics.isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append(Randomly.fromList(statistics).getName());
        return new SQLQueryAdapter(sb.toString(), true);
    }

    private static String getNewStatisticsName(MaterializeTable randomTable) {
        List<MaterializeStatisticsObject> statistics = randomTable.getStatistics();
        int i = 0;
        while (true) {
            String candidateName = "s" + i;
            if (!statistics.stream().anyMatch(stat -> stat.getName().contentEquals(candidateName))) {
                return candidateName;
            }
            i++;
        }
    }

}
