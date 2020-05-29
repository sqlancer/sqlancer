package sqlancer.mysql.ast;

import java.util.function.BinaryOperator;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.mysql.ast.MySQLCastOperation.CastType;

public class MySQLBinaryOperation implements MySQLExpression {

    private final MySQLExpression left;
    private final MySQLExpression right;
    private final MySQLBinaryOperator op;

    public enum MySQLBinaryOperator {

        AND("&") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        },
        OR("|") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        },
        XOR("^") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l ^ r);
            }
        };

        private String textRepresentation;

        private static MySQLConstant applyBitOperation(MySQLConstant left, MySQLConstant right,
                BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return MySQLConstant.createNullConstant();
            } else {
                long leftVal = left.castAs(CastType.SIGNED).getInt();
                long rightVal = right.castAs(CastType.SIGNED).getInt();
                long value = op.apply(leftVal, rightVal);
                return MySQLConstant.createUnsignedIntConstant(value);
            }
        }

        MySQLBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract MySQLConstant apply(MySQLConstant left, MySQLConstant right);

        public static MySQLBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public MySQLBinaryOperation(MySQLExpression left, MySQLExpression right, MySQLBinaryOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        MySQLConstant leftExpected = left.getExpectedValue();
        MySQLConstant rightExpected = right.getExpectedValue();

        /* workaround for https://bugs.mysql.com/bug.php?id=95960 */
        if (leftExpected.isString()) {
            String text = leftExpected.castAsString();
            while ((text.startsWith(" ") || text.startsWith("\t")) && text.length() > 0) {
                text = text.substring(1);
            }
            if (text.length() > 0 && (text.startsWith("\n") || text.startsWith("."))) {
                throw new IgnoreMeException();
            }
        }

        if (rightExpected.isString()) {
            String text = rightExpected.castAsString();
            while ((text.startsWith(" ") || text.startsWith("\t")) && text.length() > 0) {
                text = text.substring(1);
            }
            if (text.length() > 0 && (text.startsWith("\n") || text.startsWith("."))) {
                throw new IgnoreMeException();
            }
        }

        return op.apply(leftExpected, rightExpected);
    }

    public MySQLExpression getLeft() {
        return left;
    }

    public MySQLBinaryOperator getOp() {
        return op;
    }

    public MySQLExpression getRight() {
        return right;
    }

}
