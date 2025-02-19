package sqlancer.sqlite3.ast;

import sqlancer.Randomly;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3PostfixUnaryOperation implements SQLite3Expression, UnaryOperation<SQLite3Expression> {

    public enum PostfixUnaryOperator {
        ISNULL("ISNULL") {
            @Override
            public SQLite3Constant apply(SQLite3Constant expectedValue) {
                if (expectedValue.isNull()) {
                    return SQLite3Constant.createTrue();
                } else {
                    return SQLite3Constant.createFalse();
                }
            }
        },
        NOT_NULL("NOT NULL") {
            @Override
            public SQLite3Constant apply(SQLite3Constant expectedValue) {
                if (expectedValue.isNull()) {
                    return SQLite3Constant.createFalse();
                } else {
                    return SQLite3Constant.createTrue();
                }
            }

        },

        NOTNULL("NOTNULL") {

            @Override
            public SQLite3Constant apply(SQLite3Constant expectedValue) {
                if (expectedValue.isNull()) {
                    return SQLite3Constant.createFalse();
                } else {
                    return SQLite3Constant.createTrue();
                }
            }

        },
        IS_TRUE("IS TRUE") {

            @Override
            public SQLite3Constant apply(SQLite3Constant expectedValue) {
                if (expectedValue.isNull()) {
                    return SQLite3Constant.createIntConstant(0);
                }
                return SQLite3Cast.asBoolean(expectedValue);
            }
        },
        IS_FALSE("IS FALSE") {

            @Override
            public SQLite3Constant apply(SQLite3Constant expectedValue) {
                if (expectedValue.isNull()) {
                    return SQLite3Constant.createIntConstant(0);
                }
                return SQLite3UnaryOperation.UnaryOperator.NOT.apply(SQLite3Cast.asBoolean(expectedValue));
            }

        };

        private final String textRepresentation;

        PostfixUnaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String toString() {
            return getTextRepresentation();
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static SQLite3PostfixUnaryOperation.PostfixUnaryOperator getRandomOperator() {
            return Randomly.fromOptions(values());
        }

        public abstract SQLite3Constant apply(SQLite3Constant expectedValue);

    }

    private final SQLite3PostfixUnaryOperation.PostfixUnaryOperator operation;
    private final SQLite3Expression expression;

    public SQLite3PostfixUnaryOperation(SQLite3PostfixUnaryOperation.PostfixUnaryOperator operation,
            SQLite3Expression expression) {
        this.operation = operation;
        this.expression = expression;
    }

    public SQLite3PostfixUnaryOperation.PostfixUnaryOperator getOperation() {
        return operation;
    }

    @Override
    public SQLite3Expression getExpression() {
        return expression;
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        if (expression.getExpectedValue() == null) {
            return null;
        }
        return operation.apply(expression.getExpectedValue());
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return expression.getExplicitCollateSequence();
    }

    @Override
    public String getOperatorRepresentation() {
        return operation.getTextRepresentation();
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
