package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresSelect;

public final class PostgresExplainGenerator {

    private PostgresExplainGenerator() {

    }

    public static String explain(String selectStr) {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN (FORMAT JSON) ");
        sb.append(selectStr);
        return sb.toString();
    }

    public static String explainGeneral(String selectStr) {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");

        List<String> options = new ArrayList<>();
        boolean analyze = Randomly.getBoolean();
        boolean genericPlan = !analyze && Randomly.getBoolean();
        if (analyze) {
            options.add("ANALYZE");
        }
        if (genericPlan) {
            options.add("GENERIC_PLAN");
        }
        if (Randomly.getBoolean()) {
            options.add("FORMAT " + Randomly.fromOptions("TEXT", "XML", "JSON", "YAML"));
        }
        if (Randomly.getBoolean()) {
            options.add("VERBOSE");
        }
        if (Randomly.getBoolean()) {
            options.add("COSTS");
        }
        if (analyze && Randomly.getBoolean()) {
            options.add("BUFFERS");
        }
        if (analyze && Randomly.getBoolean()) {
            options.add("TIMING");
        }
        if (Randomly.getBoolean()) {
            options.add("SUMMARY");
        }
        if (!options.isEmpty()) {
            sb.append("(");
            sb.append(String.join(", ", options));
            sb.append(") ");
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
