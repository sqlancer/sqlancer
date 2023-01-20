package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBSimilarTo implements CnosDBExpression {

    private final CnosDBExpression string;
    private final CnosDBExpression similarTo;

    public CnosDBSimilarTo(CnosDBExpression string, CnosDBExpression similarTo) {
        this.string = string;
        this.similarTo = similarTo;
    }

    public CnosDBExpression getString() {
        return string;
    }

    public CnosDBExpression getSimilarTo() {
        return similarTo;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

}
