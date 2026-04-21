package sqlancer.mariadb.gen;

import java.util.Collections;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTables;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.ast.MariaDBVisitor;

public final class MariaDBDeleteGenerator extends AbstractDeleteGenerator {

    private final MariaDBSchema schema;
    private final Randomly r;

    private MariaDBDeleteGenerator(MariaDBSchema schema, Randomly r) {
        this.schema = schema;
        this.r = r;
    }

    public static SQLQueryAdapter delete(MariaDBSchema schema, Randomly r) {
        return new MariaDBDeleteGenerator(schema, r).getStatement();
    }

    @Override
    public void buildStatement() {
        MariaDBTable table = schema.getRandomTable();

        MariaDBExpressionGenerator expressionGenerator = new MariaDBExpressionGenerator(r);

        AbstractTables<MariaDBTable, MariaDBColumn> tablesAndColumns = new AbstractTables<>(
                Collections.singletonList(table));
        expressionGenerator.setTablesAndColumns(tablesAndColumns);

        errors.add("foreign key constraint fails");
        errors.add("cannot delete or update a parent row");
        errors.add("Data truncated");
        errors.add("Division by 0");
        errors.add("Incorrect value");

        sb.append("DELETE");

        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" LOW_PRIORITY");
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" IGNORE");
        }

        sb.append(" FROM ");
        sb.append(table.getName());

        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(MariaDBVisitor.asString(MariaDBExpressionGenerator.getRandomConstant(r)));
            } else {
                sb.append(MariaDBVisitor.asString(expressionGenerator.getRandomExpression()));
            }
        }

        // ORDER BY + LIMIT
        if (Randomly.getBooleanWithRatherLowProbability() && !table.getColumns().isEmpty()) {
            sb.append(" ORDER BY ");
            sb.append(Randomly.fromList(table.getColumns()).getName());
            if (Randomly.getBoolean()) {
                sb.append(Randomly.getBoolean() ? " ASC" : " DESC");
            }
        }

        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" LIMIT ");
            sb.append(Randomly.getNotCachedInteger(1, 10));
        }

        // RETURNING clause (MariaDB >= 10.5)
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" RETURNING ");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(MariaDBVisitor.asString(MariaDBExpressionGenerator.getRandomConstant(r)));
            } else {
                sb.append(MariaDBVisitor.asString(expressionGenerator.getRandomExpression()));
            }
        }

        if (sb.toString().contains("RLIKE") || sb.toString().contains("REGEXP")) {
            errors.add("Regex error");
            errors.add("quantifier does not follow a repeatable item");
            errors.add("Got error");
        }
    }
}
