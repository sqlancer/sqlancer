package sqlancer.sqlite3.ast;

import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.sqlite3.SQLite3CollateHelper;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public class SQLite3UnaryOperation extends SQLite3Expression implements UnaryOperation<SQLite3Expression> {

    private final SQLite3UnaryOperation.UnaryOperator operation;
    private final SQLite3Expression expression;

    public SQLite3UnaryOperation(SQLite3UnaryOperation.UnaryOperator operation, SQLite3Expression expression) {
        this.operation = operation;
        this.expression = expression;
    }

    // For the purposes of the previous sentence, a column name preceded by one or
    // more unary "+" operators is still considered a column name.
    @Override
    public SQLite3CollateSequence getImplicitCollateSequence() {
        if (operation == UnaryOperator.PLUS) {
            if (SQLite3CollateHelper.shouldGetSubexpressionAffinity(expression)) {
                return expression.getImplicitCollateSequence();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Supported unary prefix operators are these: -, +, ~, and NOT.
     *
     * @see <a href="https://www.sqlite.org/lang_expr.html">SQL Language Expressions</a>
     *
     */
    public enum UnaryOperator {
        MINUS("-") {
            @Override
            public SQLite3Constant apply(SQLite3Constant constant) {
                if (constant.isNull()) {
                    return SQLite3Constant.createNullConstant();
                }
                SQLite3Constant intConstant;
                if (constant.getDataType() == SQLite3DataType.TEXT
                        || constant.getDataType() == SQLite3DataType.BINARY) {
                    intConstant = SQLite3Cast.castToNumericFromNumOperand(constant);
                } else {
                    intConstant = constant;
                }
                if (intConstant.getDataType() == SQLite3DataType.INT) {
                    if (intConstant.asInt() == Long.MIN_VALUE) {
                        // SELECT - -9223372036854775808; -- 9.22337203685478e+18
                        return SQLite3Constant.createRealConstant(-(double) Long.MIN_VALUE);
                    } else {
                        return SQLite3Constant.createIntConstant(-intConstant.asInt());
                    }
                }
                if (intConstant.getDataType() == SQLite3DataType.REAL) {
                    return SQLite3Constant.createRealConstant(-intConstant.asDouble());
                }
                throw new AssertionError(intConstant);
            }
        },
        PLUS("+") {
            @Override
            public SQLite3Constant apply(SQLite3Constant constant) {
                return constant;
            }

        },
        NEGATE("~") {
            @Override
            public SQLite3Constant apply(SQLite3Constant constant) {
                SQLite3Constant intValue = SQLite3Cast.castToInt(constant);
                if (intValue.isNull()) {
                    return intValue;
                }
                return SQLite3Constant.createIntConstant(~intValue.asInt());
            }
        },
        NOT("NOT") {
            @Override
            public SQLite3Constant apply(SQLite3Constant constant) {
                Optional<Boolean> boolVal = SQLite3Cast.isTrue(constant);
                if (boolVal.isPresent()) {
                    Boolean negated = !boolVal.get();
                    return SQLite3Constant.createBoolean(negated);
                } else {
                    return SQLite3Constant.createNullConstant();
                }
            }
        };

        private String textRepresentation;

        UnaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String toString() {
            return getTextRepresentation();
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public SQLite3UnaryOperation.UnaryOperator getRandomOperator() {
            return Randomly.fromOptions(values());
        }

        public abstract SQLite3Constant apply(SQLite3Constant constant);

    }

    public SQLite3UnaryOperation.UnaryOperator getOperation() {
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
        } else {
            return operation.apply(expression.getExpectedValue());
        }
    }

    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
        return expression.getExplicitCollateSequence();
    }

    @Override
    public String getOperatorRepresentation() {
        return operation.getTextRepresentation();
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
