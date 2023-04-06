package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeBinaryBitOperation.MaterializeBinaryBitOperator;

public class MaterializeBinaryBitOperation extends
        BinaryOperatorNode<MaterializeExpression, MaterializeBinaryBitOperator> implements MaterializeExpression {

    public enum MaterializeBinaryBitOperator implements Operator {
        BITWISE_AND("&"), //
        BITWISE_OR("|"), //
        BITWISE_XOR("#"), //
        BITWISE_SHIFT_LEFT("<<"), //
        BITWISE_SHIFT_RIGHT(">>");

        private String text;

        MaterializeBinaryBitOperator(String text) {
            this.text = text;
        }

        public static MaterializeBinaryBitOperator getRandom() {
            return Randomly.fromOptions(MaterializeBinaryBitOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }

    }

    public MaterializeBinaryBitOperation(MaterializeBinaryBitOperator op, MaterializeExpression left,
            MaterializeExpression right) {
        super(left, right, op);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.INT;
    }

}
