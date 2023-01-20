package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.common.ast.BinaryOperatorNode;

public class CnosDBBinaryLogicalOperation extends BinaryOperatorNode<CnosDBExpression, BinaryLogicalOperator>
        implements CnosDBExpression {

    public CnosDBBinaryLogicalOperation(CnosDBExpression left, CnosDBExpression right, BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    public enum BinaryLogicalOperator implements BinaryOperatorNode.Operator {
        AND, OR;

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

}
