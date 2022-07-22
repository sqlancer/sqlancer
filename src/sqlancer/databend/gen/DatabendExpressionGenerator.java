package sqlancer.databend.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.*;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class DatabendExpressionGenerator extends UntypedExpressionGenerator<Node<DatabendExpression>, DatabendColumn> {

    private final DatabendGlobalState globalState;

    public DatabendExpressionGenerator(DatabendGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, CASE,
        IN, COLLATE, LIKE_ESCAPE
    }

    @Override
    protected Node<DatabendExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            DatabendAggregateFunction aggregate = DatabendAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        if (!globalState.getDbmsSpecificOptions().testCollate) {
            possibleOptions.remove(Expression.COLLATE);
        }
        if (!globalState.getDbmsSpecificOptions().testFunctions) {
            possibleOptions.remove(Expression.FUNC);
        }
        if (!globalState.getDbmsSpecificOptions().testCasts) {
            possibleOptions.remove(Expression.CAST);
        }
        if (!globalState.getDbmsSpecificOptions().testBetween) {
            possibleOptions.remove(Expression.BETWEEN);
        }
        if (!globalState.getDbmsSpecificOptions().testIn) {
            possibleOptions.remove(Expression.IN);
        }
        if (!globalState.getDbmsSpecificOptions().testCase) {
            possibleOptions.remove(Expression.CASE);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryComparisons) {
            possibleOptions.remove(Expression.BINARY_COMPARISON);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryLogicals) {
            possibleOptions.remove(Expression.BINARY_LOGICAL);
        }
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case COLLATE:
            return new NewUnaryPostfixOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    DatabendCollate.getRandom());
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    DatabendUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new NewUnaryPostfixOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    DatabendUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = DatabendBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = DatabendBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), DatabendBinaryArithmeticOperator.getRandom());
        case CAST:
            return new DatabendCastOperation(generateExpression(depth + 1),
                    DatabendCompositeDataType.getRandomWithoutNull());
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new NewFunctionNode<DatabendExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new NewBetweenOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<DatabendExpression>(generateExpression(depth + 1),
                    generateExpressions(nr, depth + 1), generateExpressions(nr, depth + 1),
                    generateExpression(depth + 1));
        case LIKE_ESCAPE:
            return new NewTernaryNode<DatabendExpression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), "LIKE", "ESCAPE");
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<DatabendExpression> generateColumn() {
        DatabendColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<DatabendExpression, DatabendColumn>(column);
    }

    @Override
    public Node<DatabendExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return DatabendConstant.createNullConstant();
        }
//        DatabendDataType type = DatabendDataType.getRandomWithoutNull();
        //TODO 先跳过varchar等待databend对其更好的支持，或自己改写case VARCHAR的代码
        DatabendDataType type = DatabendDataType.getRandomWithoutNullAndVarchar();
        switch (type) {
        case INT:
            if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }
            return DatabendConstant.createIntConstant(globalState.getRandomly().getInteger());
        case DATE:
            if (!globalState.getDbmsSpecificOptions().testDateConstants) {
                throw new IgnoreMeException();
            }
            return DatabendConstant.createDateConstant(globalState.getRandomly().getInteger());
        case TIMESTAMP:
            if (!globalState.getDbmsSpecificOptions().testTimestampConstants) {
                throw new IgnoreMeException();
            }
            return DatabendConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                throw new IgnoreMeException();
            }
            return DatabendConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                throw new IgnoreMeException();
            }
            return DatabendConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                throw new IgnoreMeException();
            }
            return DatabendConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public List<Node<DatabendExpression>> generateOrderBys() {
        List<Node<DatabendExpression>> expr = super.generateOrderBys();
        List<Node<DatabendExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<DatabendExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public static class DatabendCastOperation extends NewUnaryPostfixOperatorNode<DatabendExpression> {

        public DatabendCastOperation(Node<DatabendExpression> expr, DatabendCompositeDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    public enum DatabendAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), STRING_AGG(1), FIRST(1), SUM(1), STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private int nrArgs;

        DatabendAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static DatabendAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public enum DBFunction {
        // trigonometric functions
        ACOS(1), //
        ASIN(1), //
        ATAN(1), //
        COS(1), //
        SIN(1), //
        TAN(1), //
        COT(1), //
        ATAN2(1), //
        // math functions
        ABS(1), //
        CEIL(1), //
        CEILING(1), //
        FLOOR(1), //
        LOG(1), //
        LOG10(1), LOG2(1), //
        LN(1), //
        PI(0), //
        SQRT(1), //
        POWER(1), //
        CBRT(1), //
        ROUND(2), //
        SIGN(1), //
        DEGREES(1), //
        RADIANS(1), //
        MOD(2), //
        XOR(2), //
        // string functions
        LENGTH(1), //
        LOWER(1), //
        UPPER(1), //
        SUBSTRING(3), //
        REVERSE(1), //
        CONCAT(1, true), //
        CONCAT_WS(1, true), CONTAINS(2), //
        PREFIX(2), //
        SUFFIX(2), //
        INSTR(2), //
        PRINTF(1, true), //
        REGEXP_MATCHES(2), //
        REGEXP_REPLACE(3), //
        STRIP_ACCENTS(1), //

        // date functions
        DATE_PART(2), AGE(2),

        COALESCE(3), NULLIF(2),

        // LPAD(3),
        // RPAD(3),
        LTRIM(1), RTRIM(1),
        // LEFT(2), https://github.com/cwida/Databend/issues/633
        // REPEAT(2),
        REPLACE(3), UNICODE(1),

        BIT_COUNT(1), BIT_LENGTH(1), LAST_DAY(1), MONTHNAME(1), DAYNAME(1), YEARWEEK(1), DAYOFMONTH(1), WEEKDAY(1),
        WEEKOFYEAR(1),

        IFNULL(2), IF(3);

        private int nrArgs;
        private boolean isVariadic;

        DBFunction(int nrArgs) {
            this(nrArgs, false);
        }

        DBFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static DBFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            if (isVariadic) {
                return Randomly.smallNumber() + nrArgs;
            } else {
                return nrArgs;
            }
        }

    }

    public enum DatabendUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        DatabendUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DatabendUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static final class DatabendCollate implements Operator {

        private final String textRepr;

        private DatabendCollate(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return "COLLATE " + textRepr;
        }

        public static DatabendCollate getRandom() {
            return new DatabendCollate(DatabendTableGenerator.getRandomCollate());
        }

    }

    public enum DatabendUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        DatabendUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DatabendUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DatabendBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DatabendBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), LSHIFT("<<"), RSHIFT(">>");

        private String textRepr;

        DatabendBinaryArithmeticOperator(String textRepr) {
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

    public enum DatabendBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE"), SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO"),
        REGEX_POSIX("~"), REGEX_POSIT_NOT("!~");

        private String textRepr;

        DatabendBinaryComparisonOperator(String textRepr) {
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

    public NewFunctionNode<DatabendExpression, DatabendAggregateFunction> generateArgsForAggregate(
            DatabendAggregateFunction aggregateFunction) {
        return new NewFunctionNode<DatabendExpression, DatabendAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<DatabendExpression> generateAggregate() {
        DatabendAggregateFunction aggrFunc = DatabendAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<DatabendExpression> negatePredicate(Node<DatabendExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, DatabendUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<DatabendExpression> isNull(Node<DatabendExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, DatabendUnaryPostfixOperator.IS_NULL);
    }

}
