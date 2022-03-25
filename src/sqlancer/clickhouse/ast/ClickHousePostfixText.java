package sqlancer.clickhouse.ast;

import sqlancer.common.visitor.UnaryOperation;

/**
 * Created by Bijitashya on 03, 2022
 */
public  class ClickHousePostfixText extends ClickHouseExpression implements UnaryOperation<ClickHouseExpression> {


    private final ClickHouseExpression expr;
    private final String text;
    private ClickHouseConstant expectedValue;

    public  ClickHousePostfixText(ClickHouseExpression expr, String text, ClickHouseConstant expectedValue) {
        this.expr = expr;
        this.text = text;
        this.expectedValue = expectedValue;
    }

    public ClickHousePostfixText(String text, ClickHouseConstant expectedValue) {
        this(null, text, expectedValue);
    }

    public String getText() {
        return text;
    }

    @Override
    public ClickHouseConstant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public ClickHouseExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return getText();
    }

}
