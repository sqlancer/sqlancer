package sqlancer.postgres.gen;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import java.util.Arrays;

public final class PostgresExplainGenerator {

    private PostgresExplainGenerator() {

    }

    public static String explain(String selectStr) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN (FORMAT JSON) ");
        sb.append(selectStr);
        return sb.toString();
    }


    public static String explainGeneral(String selectStr) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        
        // Add various EXPLAIN options randomly
        if (Randomly.getBoolean()) {
            sb.append("(ANALYZE) ");
        }
        if (Randomly.getBoolean()) {
            sb.append("(FORMAT ");
            sb.append(Randomly.fromOptions("TEXT", "XML", "JSON", "YAML"));
            sb.append(") ");
        }
        if (Randomly.getBoolean()) {
            sb.append("(VERBOSE) ");
        }
        if (Randomly.getBoolean()) {
            sb.append("(COSTS) ");
        }
        if (Randomly.getBoolean()) {
            sb.append("(BUFFERS) ");
        }
        if (Randomly.getBoolean()) {
            sb.append("(TIMING) ");
        }
        if (Randomly.getBoolean()) {
            sb.append("(SUMMARY) ");
        }
        
        sb.append(selectStr);
        return sb.toString();
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) throws Exception {
        PostgresSchema.PostgresTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
        gen.setTablesAndColumns(new PostgresTables(Arrays.asList(table)));
        PostgresSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setFetchColumns(gen.generateFetchColumns(false));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(PostgresDataType.BOOLEAN));
        }
        return new SQLQueryAdapter(explainGeneral(select.asString()));
    }

}
