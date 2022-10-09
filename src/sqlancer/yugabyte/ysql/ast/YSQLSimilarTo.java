package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLSimilarTo implements YSQLExpression {

    private final YSQLExpression string;
    private final YSQLExpression similarTo;
    private final YSQLExpression escapeCharacter;

    public YSQLSimilarTo(YSQLExpression string, YSQLExpression similarTo, YSQLExpression escapeCharacter) {
        this.string = string;
        this.similarTo = similarTo;
        this.escapeCharacter = escapeCharacter;
    }

    public YSQLExpression getString() {
        return string;
    }

    public YSQLExpression getSimilarTo() {
        return similarTo;
    }

    public YSQLExpression getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BOOLEAN;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        return null;
    }

}
