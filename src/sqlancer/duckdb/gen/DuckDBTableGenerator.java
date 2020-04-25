package sqlancer.duckdb.gen;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.ast.newast.Node;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.gen.UntypedExpressionGenerator;

public class DuckDBTableGenerator {

	public Query getQuery(DuckDBGlobalState globalState) {
		Set<String> errors = new HashSet<>();
		StringBuilder sb = new StringBuilder();
		String tableName = globalState.getSchema().getFreeTableName();
		sb.append("CREATE TABLE ");
		sb.append(tableName);
		sb.append("(");
		List<DuckDBColumn> columns = getNewColumns();
		UntypedExpressionGenerator<Node<DuckDBExpression>, DuckDBColumn> gen = new DuckDBExpressionGenerator(
				globalState).setColumns(columns);
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns.get(i).getName());
			sb.append(" ");
			sb.append(columns.get(i).getType());
			if (globalState.getDmbsSpecificOptions().testCollate && Randomly.getBooleanWithRatherLowProbability() && columns.get(i).getType().getPrimitiveDataType() == DuckDBDataType.VARCHAR) {
				sb.append(" COLLATE ");
				sb.append(getRandomCollate());
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append(" UNIQUE");
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append(" NOT NULL");
			}
			if (globalState.getDmbsSpecificOptions().testCheckConstraints && Randomly.getBooleanWithRatherLowProbability()) {
				sb.append(" CHECK(");
				sb.append(DuckDBToStringVisitor.asString(gen.generateExpression()));
				DuckDBErrors.addExpressionErrors(errors);
				sb.append(")");
			}
			if (Randomly.getBoolean() && globalState.getDmbsSpecificOptions().testDefaultValues) {
				sb.append(" DEFAULT(");
				sb.append(DuckDBToStringVisitor.asString(gen.generateConstant()));
				sb.append(")");
			}
		}
		if (Randomly.getBoolean()) {
			errors.add("Invalid type for index");
			List<DuckDBColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
			sb.append(", PRIMARY KEY(");
			sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
		}
		sb.append(")");
		return new QueryAdapter(sb.toString(), errors, true);
	}

	public static String getRandomCollate() {
		return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
	}

	private static List<DuckDBColumn> getNewColumns() {
		List<DuckDBColumn> columns = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			String columnName = String.format("c%d", i);
			DuckDBCompositeDataType columnType = DuckDBCompositeDataType.getRandom();
			columns.add(new DuckDBColumn(columnName, columnType, false, false));
		}
		return columns;
	}

}
