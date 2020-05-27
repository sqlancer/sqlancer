package sqlancer.mariadb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.ast.MariaDBVisitor;

public class MariaDBUpdateGenerator {

    private MariaDBUpdateGenerator() {
    }

    public static Query update(MariaDBSchema s, Randomly r) {
        MariaDBTable randomTable = s.getRandomTable();
        StringBuilder sb = new StringBuilder("UPDATE ");
        if (Randomly.getBoolean()) {
            sb.append("LOW_PRIORITY ");
        }
        if (Randomly.getBoolean()) {
            sb.append("IGNORE ");
        }
        sb.append(randomTable.getName());
        sb.append(" SET ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(randomTable.getRandomColumn().getName());
            sb.append("=");
            if (Randomly.getBoolean()) {
                sb.append(MariaDBVisitor.asString(MariaDBExpressionGenerator.getRandomConstant(r)));
            } else {
                sb.append("DEFAULT");
            }
            // [WHERE where_condition] [ORDER BY ...] [LIMIT row_count]
        }
        List<String> errors = new ArrayList<>();
        MariaDBErrors.addInsertErrors(errors);
        return new QueryAdapter(sb.toString(), errors);
    }

}
