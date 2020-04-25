package sqlancer.tidb.gen;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public class TiDBAlterTableGenerator {

	private enum Action {
		MODIFY_COLUMN, ENABLE_DISABLE_KEYS, FORCE, DROP_PRIMARY_KEY, ADD_PRIMARY_KEY, ADD, CHANGE, DROP_COLUMN, ORDER_BY
	}

	public static Query getQuery(TiDBGlobalState globalState) {
		Set<String> errors = new HashSet<>();
		StringBuilder sb = new StringBuilder("ALTER TABLE ");
		TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(table.getColumns());
		TiDBColumn column = table.getRandomColumn();
		sb.append(table.getName());
		Action a = Randomly.fromOptions(Action.values());
		sb.append(" ");
		switch (a) {
		case MODIFY_COLUMN:
			if (true) {
				throw new IgnoreMeException();
			}
			sb.append("MODIFY ");
			sb.append(column.getName());
			sb.append(" ");
			sb.append(TiDBDataType.getRandom());
			errors.add("Unsupported modify column");
			break;
		case DROP_COLUMN:
			sb.append(" DROP ");
			if (table.getColumns().size() <= 1) {
				throw new IgnoreMeException();
			}
			sb.append(column.getName());
			errors.add("with index covered now");
			errors.add("Unsupported drop integer primary key");
			errors.add("has a generated column dependency");
			break;
		case ENABLE_DISABLE_KEYS:
			sb.append(Randomly.fromOptions("ENABLE", "DISABLE"));
			sb.append(" KEYS");
			break;
		case FORCE:
			sb.append("FORCE");
			break;
		case DROP_PRIMARY_KEY:
			if (!column.isPrimaryKey()) {
				throw new IgnoreMeException();
			}
			errors.add("Unsupported drop integer primary key");
			errors.add("Unsupported drop primary key when alter-primary-key is false");
			sb.append(" DROP PRIMARY KEY");
			break;
		case ADD_PRIMARY_KEY:
			sb.append("ADD PRIMARY KEY(");
			sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			errors.add("Unsupported add primary key, alter-primary-key is false");
			break;
		case CHANGE:
			if (true) {
				throw new IgnoreMeException();
			}
			sb.append(" CHANGE ");
			sb.append(column.getName());
			sb.append(" ");
			sb.append(column.getName());
			sb.append(" ");
			sb.append(column.getType().getPrimitiveDataType());
			sb.append(" NOT NULL ");
			errors.add("Invalid use of NULL value");
			errors.add("Unsupported modify column:");
			errors.add("Invalid integer format for value");
			break;
		case ORDER_BY:
			sb.append(" ORDER BY ");
			sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName() + Randomly.fromOptions("", " ASC", " DESC")).collect(Collectors.joining(", ")));
			break;
		}

		return new QueryAdapter(sb.toString(), errors, true);
	}

}
