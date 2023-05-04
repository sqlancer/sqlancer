package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeSimilarTo implements MaterializeExpression {

    private final MaterializeExpression string;
    private final MaterializeExpression similarTo;
    private final MaterializeExpression escapeCharacter;

    public MaterializeSimilarTo(MaterializeExpression string, MaterializeExpression similarTo,
            MaterializeExpression escapeCharacter) {
        this.string = string;
        this.similarTo = similarTo;
        this.escapeCharacter = escapeCharacter;
    }

    public MaterializeExpression getString() {
        return string;
    }

    public MaterializeExpression getSimilarTo() {
        return similarTo;
    }

    public MaterializeExpression getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        return null;
    }

}
