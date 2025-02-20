package sqlancer.clickhouse.ast;

public class ClickHouseSubquery implements ClickHouseExpression {

    private final String query;

    public ClickHouseSubquery(String query) {
        this.query = query;
    }

    public static ClickHouseExpression create(String query) {
        return new ClickHouseSubquery(query);
    }

    public String getQuery() {
        return query;
    }
}
