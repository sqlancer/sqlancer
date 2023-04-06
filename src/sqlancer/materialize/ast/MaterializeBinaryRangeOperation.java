package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeBinaryRangeOperation extends BinaryNode<MaterializeExpression>
        implements MaterializeExpression {

    private final String op;

    public enum MaterializeBinaryRangeOperator implements Operator {
        UNION("+"), INTERSECTION("*"), DIFFERENCE("-");

        private final String textRepresentation;

        MaterializeBinaryRangeOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static MaterializeBinaryRangeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum MaterializeBinaryRangeComparisonOperator {
        CONTAINS_RANGE_OR_ELEMENT("@>"), RANGE_OR_ELEMENT_IS_CONTAINED("<@"), OVERLAP("&&"), STRICT_LEFT_OF("<<"),
        STRICT_RIGHT_OF(">>");

        private final String textRepresentation;

        MaterializeBinaryRangeComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static MaterializeBinaryRangeComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public MaterializeBinaryRangeOperation(MaterializeBinaryRangeComparisonOperator op, MaterializeExpression left,
            MaterializeExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    public MaterializeBinaryRangeOperation(MaterializeBinaryRangeOperator op, MaterializeExpression left,
            MaterializeExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

    @Override
    public String getOperatorRepresentation() {
        return op;
    }

}
