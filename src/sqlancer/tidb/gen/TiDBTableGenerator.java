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
import sqlancer.tidb.TiDBBugs;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;
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
		
		if (Randomly.getBoolean() && globalState.getSchema().getDatabaseTables().size() > 0) {
			sb.append(" LIKE ");
			TiDBTable otherTable = globalState.getSchema().getRandomTable();
			sb.append(otherTable.getName());
		} else {
			createNewTable(gen, sb);
		}
		return new QueryAdapter(sb.toString(), errors, true);
	}

	private void createNewTable(TiDBExpressionGenerator gen, StringBuilder sb) {
		sb.append("(");
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns.get(i).getName());
			sb.append(" ");
			TiDBDataType type = TiDBDataType.getRandom();
			sb.append(type);
			appendSpecifiers(sb, type);
			appendSizeSpecifiers(sb, type);
			sb.append(" ");
			boolean isGeneratedColumn = Randomly.getBooleanWithRatherLowProbability();
			if (isGeneratedColumn && !TiDBBugs.BUG_16020) {
				sb.append(" AS (");
				sb.append(TiDBVisitor.asString(gen.generateExpression()));
				sb.append(") ");
				sb.append(Randomly.fromOptions("STORED", "VIRTUAL"));
				sb.append(" ");
				errors.add("Generated column can refer only to generated columns defined prior to it");
				errors.add("'Defining a virtual generated column as primary key' is not supported for generated columns.");
				errors.add("contains a disallowed function.");
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append("CHECK (");
				sb.append(TiDBVisitor.asString(gen.generateExpression()));
				sb.append(") ");
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append("NOT NULL ");
			}
			if (Randomly.getBoolean() && type != TiDBDataType.TEXT && !isGeneratedColumn) {
				sb.append("DEFAULT ");
				sb.append(TiDBVisitor.asString(gen.generateConstant()));
				sb.append(" ");
				errors.add("Invalid default value");
				errors.add("All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead");
			}
			if (type == TiDBDataType.INT && Randomly.getBooleanWithRatherLowProbability()) {
				sb.append(" AUTO_INCREMENT ");
				errors.add("there can be only one auto column and it must be defined as a key");
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
		if (Randomly.getBooleanWithRatherLowProbability()) {
			sb.append("PARTITION BY HASH(");
			sb.append(TiDBVisitor.asString(gen.generateExpression()));
			sb.append(") ");
			sb.append("PARTITIONS ");
			sb.append(Randomly.getNotCachedInteger(1, 100));
			errors.add("Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed");
			errors.add("This partition function is not allowed");
			errors.add("A PRIMARY KEY must include all columns in the table's partitioning function");
			errors.add("A UNIQUE INDEX must include all columns in the table's partitioning function");
			errors.add("is of a not allowed type for this type of partitioning");
			errors.add("The PARTITION function returns the wrong type");
		}
	}

	private void appendSizeSpecifiers(StringBuilder sb, TiDBDataType type) {
		if (type.isNumeric() && Randomly.getBoolean() && !TiDBBugs.BUG_16028) {
			sb.append(" UNSIGNED");
		}
		if (type.isNumeric() && Randomly.getBoolean() && !TiDBBugs.BUG_16028 /* seems to be the same bug as https://github.com/pingcap/tidb/issues/16028 */) {
			sb.append(" ZEROFILL");
		}		
	}

	static void appendSpecifiers(StringBuilder sb, TiDBDataType type) {
		if (type == TiDBDataType.TEXT) {
			sb.append("(");
			sb.append(Randomly.getNotCachedInteger(1, 500));
			sb.append(")");
		}
	}
}
