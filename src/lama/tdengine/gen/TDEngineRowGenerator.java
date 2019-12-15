package lama.tdengine.gen;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.tdengine.TDEngineSchema.TDEngineColumn;
import lama.tdengine.TDEngineSchema.TDEngineTable;

public class TDEngineRowGenerator {

	private final StringBuilder sb = new StringBuilder();
	private final TDEngineTable randomTable;
	private final Randomly r;
	private final List<String> errors = new ArrayList<>();

	public TDEngineRowGenerator(TDEngineTable randomTable, Randomly r) {
		this.randomTable = randomTable;
		this.r = r;
	}

	public static Query insertRow(TDEngineTable randomTable, Connection con, Randomly r) {
		return new TDEngineRowGenerator(randomTable, r).generate();
	}

	private Query generate() {
		errors.add("primary timestamp column can not be null");
		sb.append("INSERT INTO ");
		sb.append(randomTable.getName());
		List<TDEngineColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
		if (columns.size() != randomTable.getColumns().size() || Randomly.getBoolean()) {
			sb.append("(");
			appendColumnNames(columns, sb);
			sb.append(")");
		} else {
			columns = randomTable.getColumns(); // get them again in sorted order
		}
		sb.append(" VALUES ");
		int nrRows = 1 + Randomly.smallNumber();
		appendNrValues(sb, columns, nrRows);

		return new QueryAdapter(sb.toString(), errors);
	}

	private void appendNrValues(StringBuilder sb, List<TDEngineColumn> columns, int nrValues) {
		for (int i = 0; i < nrValues; i++) {
			sb.append("(");
			appendValue(sb, columns);
			sb.append(")");
		}
	}

	private void appendValue(StringBuilder sb, List<TDEngineColumn> columns) {
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
//			SQLite3Expression literal;
//			if (columns.get(i).isIntegerPrimaryKey()) {
//				literal = SQLite3Constant.createIntConstant(r.getInteger());
//			} else {
//				if (Randomly.getBoolean()) {
//					literal = new SQLite3ExpressionGenerator(r).expectedErrors(errors).getRandomExpression();
//				} else {
//					literal = SQLite3ExpressionGenerator.getRandomLiteralValue(r);
//				}
//			}
//			SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
//			visitor.visit(literal);
//			sb.append(visitor.get());
			if (Randomly.getBoolean()) {
				sb.append("NULL");
			} else {
				switch (columns.get(i).getColumnType()) {
				case TIMESTAMP:
					sb.append(0);
					break;
				case INT:
					sb.append(r.getInteger());
					errors.add("syntax error"); // TODO exceeding data type size
					break;
				case BOOL:
					sb.append(Randomly.fromOptions("true", "false"));
					break;
				case DOUBLE:
				case FLOAT:
					sb.append(r.getDouble());
					break;
				case TEXT:
					sb.append("'");
					String replace = r.getString().replace("'", "\\'");
					sb.append(replace);
					sb.append("'");
					errors.add("syntax error");
					errors.add("invalid SQL");
					break;
				default:
					throw new AssertionError();
				}
			}
		}
	}

	private static List<TDEngineColumn> appendColumnNames(List<TDEngineColumn> columns, StringBuilder sb) {
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns.get(i).getName());
		}
		return columns;
	}

}
