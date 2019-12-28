package lama.postgres.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresGlobalState;
import lama.postgres.PostgresProvider;
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.PostgresSchema.PostgresTable;
import lama.postgres.PostgresVisitor;
import lama.postgres.ast.PostgresExpression;

public class PostgresInsertGenerator {

	public static Query insert(PostgresGlobalState globalState) {
		PostgresTable table = globalState.getSchema().getRandomTable(t -> t.isInsertable());
		List<String> errors = new ArrayList<>();
		if (PostgresProvider.IS_POSTGRES_TWELVE) {
			errors.add("cannot insert into column");
		}
		PostgresCommon.addCommonExpressionErrors(errors);
		PostgresCommon.addCommonInsertUpdateErrors(errors);
		PostgresCommon.addCommonExpressionErrors(errors);
		errors.add("multiple assignments to same column");
		errors.add("violates foreign key constraint");
		errors.add("value too long for type character varying");
		errors.add("conflicting key value violates exclusion constraint");
		errors.add("violates not-null constraint");
		errors.add("current transaction is aborted");
		errors.add("bit string too long");
		errors.add("new row violates check option for view");
		errors.add("reached maximum value of sequence");
		errors.add("but expression is of type");
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(table.getName());
		List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();
		sb.append("(");
		sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
		sb.append(")");
		if (Randomly.getBoolean()) {
			sb.append(" OVERRIDING");
			sb.append(" ");
			sb.append(Randomly.fromOptions("SYSTEM", "USER"));
			sb.append(" VALUE");
		}
		sb.append(" VALUES");

		if (Randomly.getBooleanWithSmallProbability()) {
			// bulk insert
			StringBuilder sbRowValue = new StringBuilder();
			sbRowValue.append("(");
			for (int i = 0; i < columns.size(); i++) {
				if (i != 0) {
					sbRowValue.append(", ");
				}
				sbRowValue.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateConstant(globalState.getRandomly(), columns.get(i).getColumnType())));
			}
			sbRowValue.append(")");
			
			int n = (int) Randomly.getNotCachedInteger(100, 1000);
			for (int i = 0; i < n; i++) {
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(sbRowValue);
			}
		} else {
			int n = Randomly.smallNumber() + 1;
			for (int i = 0; i < n; i++) {
				if (i != 0) {
					sb.append(", ");
				}
				insertRow(globalState.getRandomly(), sb, columns, n == 1);
			}
		}
		if (Randomly.getBoolean()) {
			sb.append(" ON CONFLICT ");
			if (Randomly.getBoolean()) {
				sb.append("(");
				sb.append(table.getRandomColumn().getName());
				sb.append(")");
				errors.add("there is no unique or exclusion constraint matching the ON CONFLICT specification");
			}
			sb.append(" DO NOTHING");
		}
		errors.add("duplicate key value violates unique constraint");
		errors.add("identity column defined as GENERATED ALWAYS");
		errors.add("out of range");
		errors.add("violates check constraint");
		errors.add("no partition of relation");
		errors.add("invalid input syntax");
		errors.add("division by zero");
		errors.add("violates foreign key constraint");
		errors.add("data type unknown");
		return new QueryAdapter(sb.toString(), errors);
	}

	private static void insertRow(Randomly r, StringBuilder sb, List<PostgresColumn> columns, boolean canBeDefault) {
		sb.append("(");
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			if (!Randomly.getBooleanWithSmallProbability() || !canBeDefault) {
				PostgresExpression generateConstant;
				if (Randomly.getBoolean()) {
					generateConstant = PostgresExpressionGenerator.generateConstant(r, columns.get(i).getColumnType());
				} else {
					generateConstant = new PostgresExpressionGenerator(r)
							.generateExpression(columns.get(i).getColumnType());
				}
				sb.append(PostgresVisitor.asString(generateConstant));
			} else {
				sb.append("DEFAULT");
			}
		}
		sb.append(")");
	}

}
