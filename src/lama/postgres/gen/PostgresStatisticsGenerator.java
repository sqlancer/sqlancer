package lama.postgres.gen;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresGlobalState;
import lama.postgres.PostgresProvider;
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.PostgresSchema.PostgresStatisticsObject;
import lama.postgres.PostgresSchema.PostgresTable;

public class PostgresStatisticsGenerator {

	public static Query insert(PostgresGlobalState globalState) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE STATISTICS ");
		if (Randomly.getBoolean()) {
			sb.append(" IF NOT EXISTS");
		}
		PostgresTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView()); // TODO materialized view
		if (randomTable.getColumns().size() < 2) {
			throw new IgnoreMeException();
		}
		sb.append(" ");
		sb.append(getNewStatisticsName(randomTable));
		if (Randomly.getBoolean()) {
			sb.append(" (");
			List<String> statsSubset;
			if (PostgresProvider.IS_POSTGRES_TWELVE) {
				statsSubset = Randomly.nonEmptySubset("ndistinct", "dependencies", "mcv");
			} else {
				statsSubset = Randomly.nonEmptySubset("ndistinct", "dependencies");
			}
			sb.append(statsSubset.stream().collect(Collectors.joining(", ")));
			sb.append(")");
		}
		
		List<PostgresColumn> randomColumns = randomTable.getRandomNonEmptyColumnSubset(globalState.getRandomly().getInteger(2, randomTable.getColumns().size()));
		sb.append(" ON ");
		sb.append(randomColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
		sb.append(" FROM ");
		sb.append(randomTable.getName());
		return new QueryAdapter(sb.toString(), Arrays.asList("cannot have more than 8 columns in statistics"), true);
	}
	
	public static Query remove(PostgresGlobalState globalState) {
		StringBuilder sb = new StringBuilder("DROP STATISTICS ");
		PostgresTable randomTable = globalState.getSchema().getRandomTable();
		List<PostgresStatisticsObject> statistics = randomTable.getStatistics();
		if (statistics.isEmpty()) {
			throw new IgnoreMeException();
		}
		sb.append(Randomly.fromList(statistics).getName());
		return new QueryAdapter(sb.toString(), true);
	}

	private static String getNewStatisticsName(PostgresTable randomTable) {
		List<PostgresStatisticsObject> statistics = randomTable.getStatistics();
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
