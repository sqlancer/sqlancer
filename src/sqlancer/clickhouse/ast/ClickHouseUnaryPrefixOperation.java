package sqlancer.clickhouse.ast;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.visitor.UnaryOperation;

public class ClickHouseUnaryPrefixOperation extends ClickHouseExpression
        implements UnaryOperation<ClickHouseExpression> {
    private final ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator operation;
    private final ClickHouseExpression expression;

    public ClickHouseUnaryPrefixOperation(ClickHouseExpression expression, ClickHouseUnaryPrefixOperator operation) {
        this.operation = operation;
        this.expression = expression;
    }

    public enum ClickHouseUnaryPrefixOperator implements Operator {
        NOT("NOT") {
            @Override
            public ClickHouseConstant apply(ClickHouseConstant constant) {
                if (constant.getDataType() == ClickHouseDataType.Nothing) {
                    return ClickHouseConstant.createNullConstant();
                } else {
                    return constant.asBooleanNotNull() ? ClickHouseConstant.createFalse()
                            : ClickHouseConstant.createTrue();
                }
            }
        },
        MINUS("-") {
            @Override
            public ClickHouseConstant apply(ClickHouseConstant constant) {
                if (constant.getDataType() == ClickHouseDataType.Int32) {
                    return ClickHouseConstant.createInt32Constant(-constant.asInt());
                }
                throw new AssertionError(constant);
            }
        };

        private String s;

        ClickHouseUnaryPrefixOperator(String s) {
            this.s = s;
        }

        public static ClickHouseUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return s;
        }

        public abstract ClickHouseConstant apply(ClickHouseConstant constant);
    }

    @Override
    public ClickHouseExpression getExpression() {
        return expression;
    }

    @Override
    public String getOperatorRepresentation() {
        return operation.getTextRepresentation();
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return false;
    }

    @Override
    public ClickHouseConstant getExpectedValue() {
        if (expression.getExpectedValue() == null) {
            return null;
        } else {
            return operation.apply(expression.getExpectedValue());
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }
}
