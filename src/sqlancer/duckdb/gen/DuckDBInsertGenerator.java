package sqlancer.duckdb.gen;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.gen.AbstractInsertGenerator;

public class DuckDBInsertGenerator extends AbstractInsertGenerator<DuckDBColumn> {
	
	private DuckDBGlobalState globalState;
	private final Set<String> errors = new HashSet<>();

	public DuckDBInsertGenerator(DuckDBGlobalState globalState) {
		this.globalState = globalState;
	}

	public static Query getQuery(DuckDBGlobalState globalState) {
		return new DuckDBInsertGenerator(globalState).generate();
	}

	private Query generate() {
		sb.append("INSERT INTO ");
		DuckDBTable table = globalState.getSchema().getRandomTable();
		List<DuckDBColumn> columns = table.getRandomNonEmptyColumnSubset();
		sb.append(table.getName());
		sb.append("(");
		sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
		sb.append(")");
		sb.append(" VALUES ");
		insertColumns(columns);
		errors.add("NOT NULL constraint failed");
		errors.add("PRIMARY KEY or UNIQUE constraint violated");
		errors.add("duplicate key value violates primary key or unique constraint");
		errors.add("can't be cast because the value is out of range for the destination type");
		errors.add("Could not convert string");
		errors.add("timestamp field value out of range");
		
		errors.add("Not implemented: Unimplemented type for cast"); // TODO: report?
		
		return new QueryAdapter(sb.toString(), errors);
	}

	@Override
	protected void insertValue(DuckDBColumn tiDBColumn) {
		// TODO: select a more meaningful value
		sb.append(DuckDBToStringVisitor.asString(new DuckDBExpressionGenerator(globalState).generateConstant()));
	}

}
