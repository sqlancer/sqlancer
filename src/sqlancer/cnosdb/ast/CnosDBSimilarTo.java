package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBSimilarTo implements CnosDBExpression {

    private final CnosDBExpression string;
    private final CnosDBExpression similarTo;
    private final CnosDBExpression escapeCharacter;

    public CnosDBSimilarTo(CnosDBExpression string, CnosDBExpression similarTo, CnosDBExpression escapeCharacter) {
        this.string = string;
        this.similarTo = similarTo;
        this.escapeCharacter = escapeCharacter;
    }

    public CnosDBExpression getString() {
        return string;
    }

    public CnosDBExpression getSimilarTo() {
        return similarTo;
    }

    public CnosDBExpression getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        return null;
    }

}
