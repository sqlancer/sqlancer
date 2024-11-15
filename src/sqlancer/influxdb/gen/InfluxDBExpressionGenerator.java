package sqlancer.influxdb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.influxdb.InfluxDBProvider.InfluxDBGlobalState;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBColumn;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBDataType;
import sqlancer.influxdb.ast.InfluxDBBinaryOperation;
import sqlancer.influxdb.ast.InfluxDBColumnReference;
import sqlancer.influxdb.ast.InfluxDBConstant;
import sqlancer.influxdb.ast.InfluxDBExpression;
import sqlancer.influxdb.ast.InfluxDBUnaryPostfixOperation;
import sqlancer.influxdb.ast.InfluxDBUnaryPrefixOperation;


public class InfluxDBExpressionGenerator extends UntypedExpressionGenerator<InfluxDBExpression, InfluxDBColumn> {
    private final InfluxDBGlobalState globalState;

    public InfluxDBExpressionGenerator(InfluxDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC
    }

    @Override
    public InfluxDBExpression negatePredicate(InfluxDBExpression predicate) {
        return new InfluxDBUnaryPrefixOperation(predicate, InfluxDBUnaryPrefixOperator.NOT);
    }

    @Override
    public InfluxDBExpression isNull(InfluxDBExpression expr) {
        return new InfluxDBUnaryPostfixOperation(expr, InfluxDBUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public InfluxDBExpression generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return InfluxDBConstant.createNullConstant();
        }
        InfluxDBDataType type = InfluxDBDataType.getRandomWithoutNull();
        switch (type) {
        case INT:
            return InfluxDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case BOOLEAN:
            return InfluxDBConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            return InfluxDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        case STRING:
            return InfluxDBConstant.createStringConstant(globalState.getRandomly().getString());
        default:
            throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    protected InfluxDBExpression generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case UNARY_PREFIX:
            return new InfluxDBUnaryPrefixOperation(generateExpression(depth + 1),
                    InfluxDBUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new InfluxDBUnaryPostfixOperation(generateExpression(depth + 1),
                    InfluxDBUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            return new InfluxDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    InfluxDBBinaryComparisonOperator.getRandom());
        case BINARY_ARITHMETIC:
            return new InfluxDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    InfluxDBBinaryArithmeticOperator.getRandom());
        case BINARY_LOGICAL:
            return new InfluxDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    InfluxDBBinaryLogicalOperator.getRandom());
        default:
            throw new AssertionError("Expression generation failed, depth=" + depth);
        }
    }

    @Override
    protected InfluxDBExpression generateColumn() {
        InfluxDBColumn column = Randomly.fromList(columns);
        return new InfluxDBColumnReference(column);
    }

    public enum InfluxDBUnaryPostfixOperator implements Operator {
        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");
        private final String textRepr;
        InfluxDBUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }
        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
        public static InfluxDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum InfluxDBUnaryPrefixOperator implements Operator {
        NOT("NOT");
        private final String textRepr;
        InfluxDBUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }
        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
        public static InfluxDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum InfluxDBBinaryLogicalOperator implements Operator {
        AND, OR;
        @Override
        public String getTextRepresentation() {
            return toString();
        }
        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum InfluxDBBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER_THAN(">"), GREATER_THAN_EQUALS(">="), LESS_THAN("<"), LESS_THAN_EQUALS("<="),
        NOT_EQUALS("!=");
        private final String textRepr;
        InfluxDBBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }
        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }
        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum InfluxDBBinaryArithmeticOperator implements Operator {
        ADD("+"), SUB("-"), MULT("*"), DIV("/");
        private final String textRepr;
        InfluxDBBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }
        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }
        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }
}