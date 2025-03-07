package sqlancer.postgres.gen;

public final class PostgresExplainGenerator {

    private PostgresExplainGenerator() {

    }

    public static String explain(String selectStr) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        sb.append(selectStr);
        return sb.toString();
    }

}
