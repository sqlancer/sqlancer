package lama.mariadb.gen;

import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mariadb.MariaDBErrors;
import lama.mariadb.MariaDBSchema;
import lama.mariadb.MariaDBSchema.MariaDBTable;
import lama.mariadb.ast.MariaDBVisitor;

public class MariaDBInsertGenerator {
	
	public static Query insert(MariaDBSchema s, Randomly r) {
		MariaDBTable randomTable = s.getRandomTable();
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(randomTable.getName());
		sb.append(" VALUES (");
		for (int i = 0; i < randomTable.getColumns().size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			if (Randomly.getBooleanWithSmallProbability()) {
				sb.append(MariaDBVisitor.asString(MariaDBExpressionGenerator.getRandomConstant(r)));
			} else {
				sb.append(MariaDBVisitor.asString(MariaDBExpressionGenerator.getRandomConstant(r, randomTable.getColumns().get(i).getColumnType())));
			}
		}
		sb.append(")");
		List<String> errors = new ArrayList<>();
		MariaDBErrors.addInsertErrors(errors);
		return new QueryAdapter(sb.toString(), errors);
	}

}
