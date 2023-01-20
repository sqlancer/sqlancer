package sqlancer.cnosdb.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBBinaryArithmeticOperation.CnosDBBinaryOperator;
import sqlancer.common.ast.BinaryOperatorNode;

public class CnosDBBinaryArithmeticOperation extends BinaryOperatorNode<CnosDBExpression, CnosDBBinaryOperator>
        implements CnosDBExpression {

    public CnosDBBinaryArithmeticOperation(CnosDBExpression left, CnosDBExpression right, CnosDBBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.INT;
    }

    public enum CnosDBBinaryOperator implements BinaryOperatorNode.Operator {

        ADDITION("+") {
        },
        SUBTRACTION("-") {
        },
        MULTIPLICATION("*") {
        },
        DIVISION("/") {

        },
        MODULO("%") {
        },
        EXPONENTIATION("^") {
        };

        private final String textRepresentation;

        CnosDBBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static CnosDBBinaryOperator getRandom(CnosDBDataType dataType) {
            List<CnosDBBinaryOperator> ops = new ArrayList<>(Arrays.asList(values()));
            switch (dataType) {
            case DOUBLE:
            case UINT:
            case STRING:
                ops.remove(EXPONENTIATION);
                ops.remove(MODULO);
                break;
            default:
                break;
            }

            return Randomly.fromList(ops);
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

}
