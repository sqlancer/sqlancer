package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBTableGenerator {
	
	private boolean allowPrimaryKey;
	private final List<TiDBColumn> columns = new ArrayList<>();
	private boolean primaryKeyAsTableConstraints;
	private final Set<String> errors = new HashSet<>();

	public Query getQuery(TiDBGlobalState globalState) throws SQLException {
		String tableName = globalState.getSchema().getFreeTableName();
		int nrColumns = Randomly.smallNumber() + 1;
		allowPrimaryKey = Randomly.getBoolean();
		primaryKeyAsTableConstraints = allowPrimaryKey && Randomly.getBoolean();
		for (int i = 0; i < nrColumns; i++) {
			TiDBColumn fakeColumn = new TiDBColumn("c" + i, null, false, false);
			columns.add(fakeColumn);
		}
		TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(columns);
		
		StringBuilder sb = new StringBuilder("CREATE TABLE ");
		sb.append(tableName);
		sb.append("(");
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns.get(i).getName());
			sb.append(" ");
			TiDBDataType type = TiDBDataType.getRandom();
			sb.append(type);
			appendTextSizeSpecifier(sb, type);
			if (type == TiDBDataType.INT && Randomly.getBoolean()) {
				sb.append(" UNSIGNED");
			}
			sb.append(" ");
			boolean isGeneratedColumn = Randomly.getBooleanWithRatherLowProbability();
			if (isGeneratedColumn && false) {
				// TODO: https://github.com/pingcap/tidb/issues/15733
				sb.append(" AS (");
				sb.append(TiDBVisitor.asString(gen.generateExpression()));
				sb.append(") ");
				sb.append(Randomly.fromOptions("STORED", "VIRTUAL"));
				sb.append(" ");
				errors.add("Generated column can refer only to generated columns defined prior to it");
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append("CHECK (");
				sb.append(TiDBVisitor.asString(gen.generateExpression()));
				sb.append(") ");
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append("NOT NULL ");
			}
			if (Randomly.getBoolean() && type != TiDBDataType.TEXT && false ) {
				// TODO: https://github.com/pingcap/tidb/issues/15733
				sb.append("DEFAULT ");
				sb.append(TiDBVisitor.asString(gen.generateConstant()));
				sb.append(" ");
				errors.add("Invalid default value");
			}
			if (Randomly.getBooleanWithRatherLowProbability()  && (type != TiDBDataType.TEXT)) {
				sb.append("UNIQUE ");
			}
			if (Randomly.getBooleanWithRatherLowProbability() && allowPrimaryKey && !primaryKeyAsTableConstraints  && (type != TiDBDataType.TEXT) && !isGeneratedColumn) {
				sb.append("PRIMARY KEY ");
				allowPrimaryKey = false;
			}
		}
		if (primaryKeyAsTableConstraints) {
			sb.append(", PRIMARY KEY(");
			sb.append(Randomly.nonEmptySubset(columns).stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			// TODO: do nto include blob/text columns here
			errors.add(" used in key specification without a key length");
		}
		sb.append(")");
		return new QueryAdapter(sb.toString(), errors, true);
	}

	static void appendTextSizeSpecifier(StringBuilder sb, TiDBDataType type) {
		if (type == TiDBDataType.TEXT) {
			sb.append("(500)");
		}
	}
}
