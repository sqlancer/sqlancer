package sqlancer.oxla.ast;

import sqlancer.common.ast.BinaryOperatorNode;

public abstract class OxlaOperator implements BinaryOperatorNode.Operator {
    public final String textRepresentation;
    public final OxlaTypeOverload overload;

    public OxlaOperator(String textRepresentation, OxlaTypeOverload overload) {
        this.textRepresentation = textRepresentation;
        this.overload = overload;
    }

    @Override
    public String getTextRepresentation() {
        return textRepresentation;
    }
}
