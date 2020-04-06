package sqlancer.clickhouse.gen;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.clickhouse.ClickhouseProvider.ClickhouseGlobalState;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseColumn;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseTable;
import sqlancer.gen.AbstractInsertGenerator;

public class ClickhouseInsertGenerator extends AbstractInsertGenerator<ClickhouseColumn> {
	
	private ClickhouseGlobalState globalState;
	private Set<String> errors = new HashSet<>();

	public ClickhouseInsertGenerator(ClickhouseGlobalState globalState) {
		this.globalState = globalState;
	}
	
	public static Query getQuery(ClickhouseGlobalState globalState) throws SQLException {
		return new ClickhouseInsertGenerator(globalState).get();
	}
	
	private Query get() {
		ClickhouseTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		List<ClickhouseColumn> columns = table.getRandomNonEmptyColumnSubset();
		sb.append("INSERT INTO ");
		sb.append(table.getName());
		sb.append("(");
		sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
		sb.append(")");
		sb.append(" VALUES ");
		insertColumns(columns);
		return new QueryAdapter(sb.toString(), errors);
	}

	@Override
	protected void insertValue(ClickhouseColumn tiDBColumn) {
		sb.append(globalState.getRandomly().getInteger());
	}
	
}
