package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresSimilarTo implements PostgresExpression {

    private final PostgresExpression string;
    private final PostgresExpression similarTo;
    private final PostgresExpression escapeCharacter;

    public PostgresSimilarTo(PostgresExpression string, PostgresExpression similarTo,
            PostgresExpression escapeCharacter) {
        this.string = string;
        this.similarTo = similarTo;
        this.escapeCharacter = escapeCharacter;
    }

    public PostgresExpression getString() {
        return string;
    }

    public PostgresExpression getSimilarTo() {
        return similarTo;
    }

    public PostgresExpression getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        return null;
    }

}
