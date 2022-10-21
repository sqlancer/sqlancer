package sqlancer.clickhouse.ast;

public class ClickHouseAliasOperation extends ClickHouseExpression {

    private final ClickHouseExpression expression;
    private final String alias;

    public ClickHouseAliasOperation(ClickHouseExpression expression, String alias) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.alias = alias;
    }

    @Override
    public ClickHouseConstant getExpectedValue() {
        return expression.getExpectedValue();
    }

    public ClickHouseExpression getExpression() {
        return expression;
    }

    public String getAlias() {
        return alias;
    }
}
