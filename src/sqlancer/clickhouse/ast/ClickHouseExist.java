package sqlancer.clickhouse.ast;

public class ClickHouseExist implements ClickHouseExpression {

    private final ClickHouseExpression select;

    public ClickHouseExist(ClickHouseExpression select) {
        this.select = select;
    }

    public ClickHouseExpression getExpression() {
        return select;
    }
}
