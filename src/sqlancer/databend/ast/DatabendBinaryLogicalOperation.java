package sqlancer.databend.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;

public class DatabendBinaryLogicalOperation extends NewBinaryOperatorNode<DatabendExpression> {

    public DatabendBinaryLogicalOperation(Node<DatabendExpression> left, Node<DatabendExpression> right,
                                          DatabendBinaryLogicalOperator op) {
        super(left,right,op);
    }

    public enum DatabendBinaryLogicalOperator implements BinaryOperatorNode.Operator {
        AND("AND", "and") {
//            @Override
//            public DatabendNoRECConstant apply(DatabendNoRECConstant left, DatabendNoRECConstant right) {
//                if (left.isNull() && right.isNull()) {
//                    return DatabendNoRECConstant.createNullConstant();
//                } else if (left.isNull()) {
//                    if (right.asBooleanNotNull()) {
//                        return DatabendNoRECConstant.createNullConstant();
//                    } else {
//                        return DatabendNoRECConstant.createFalse();
//                    }
//                } else if (right.isNull()) {
//                    if (left.asBooleanNotNull()) {
//                        return DatabendNoRECConstant.createNullConstant();
//                    } else {
//                        return DatabendNoRECConstant.createFalse();
//                    }
//                } else {
//                    return left.asBooleanNotNull() && right.asBooleanNotNull() ? DatabendNoRECConstant.createTrue()
//                            : DatabendNoRECConstant.createFalse();
//                }
//            }
        },
        OR("OR", "or") {
//            @Override
//            public ClickHouseConstant apply(ClickHouseConstant left, ClickHouseConstant right) {
//                if (!left.isNull() && left.asBooleanNotNull()) {
//                    return ClickHouseConstant.createTrue();
//                } else if (!right.isNull() && right.asBooleanNotNull()) {
//                    return ClickHouseConstant.createTrue();
//                } else if (left.isNull() || right.isNull()) {
//                    return ClickHouseConstant.createNullConstant();
//                } else {
//                    return ClickHouseConstant.createFalse();
//                }
//            }
        };

        private final String[] textRepresentations;

        DatabendBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public DatabendBinaryLogicalOperator getRandomOp() {
            return Randomly.fromOptions(values());
        }

//        public abstract DatabendNoRECConstant apply(DatabendNoRECConstant left, DatabendNoRECConstant right);

        public static DatabendBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

}
