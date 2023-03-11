package sqlancer.doris.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.*;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.DorisSchema.DorisDataType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class DorisExpressionGenerator extends UntypedExpressionGenerator<Node<DorisExpression>, DorisColumn> {

    private final DorisGlobalState globalState;

    public DorisExpressionGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, CASE,
        IN, COLLATE, LIKE_ESCAPE
    }

    @Override
    protected Node<DorisExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            DorisAggregateFunction aggregate = DorisAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
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
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    DorisUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new NewUnaryPostfixOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    DorisUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = DorisBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = DorisBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), DorisBinaryArithmeticOperator.getRandom());
        case CAST:
            return new DorisCastOperation(generateExpression(depth + 1),
                    DorisCompositeDataType.getRandomWithoutNull());
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new NewFunctionNode<DorisExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new NewBetweenOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<DorisExpression>(generateExpression(depth + 1),
                    generateExpressions(nr, depth + 1), generateExpressions(nr, depth + 1),
                    generateExpression(depth + 1));
        case LIKE_ESCAPE:
            return new NewTernaryNode<DorisExpression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), "LIKE", "ESCAPE");
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<DorisExpression> generateColumn() {
        DorisColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<DorisExpression, DorisColumn>(column);
    }

    @Override
    public Node<DorisExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return DorisConstant.createNullConstant();
        }
        DorisDataType type = DorisDataType.getRandomWithoutNull();
        switch (type) {
        case INT:
            if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }
            return DorisConstant.createIntConstant(globalState.getRandomly().getInteger());
        case DATE:
            if (!globalState.getDbmsSpecificOptions().testDateConstants) {
                throw new IgnoreMeException();
            }
            return DorisConstant.createDateConstant(globalState.getRandomly().getInteger());
        case DATETIME:
            if (!globalState.getDbmsSpecificOptions().testDateTimeConstants) {
                throw new IgnoreMeException();
            }
            return DorisConstant.createDatetimeConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                throw new IgnoreMeException();
            }
            return DorisConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                throw new IgnoreMeException();
            }
            return DorisConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                throw new IgnoreMeException();
            }
            return DorisConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    public Node<DorisExpression> generateConstant(DorisGlobalState globalState, DorisSchema.DorisDataType columnDataType, boolean isNullable) {
        if (isNullable && Randomly.getBooleanWithSmallProbability()) {
            return DorisConstant.createNullConstant();
        }
        long timestamp;
        SimpleDateFormat dateFormat;
        String textRepr;
        switch (columnDataType) {
            case INT:
                if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                    throw new IgnoreMeException();
                }
                return DorisConstant.createIntConstant(globalState.getRandomly().getInteger());
            case DECIMAL:
            case FLOAT:
                if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                    throw new IgnoreMeException();
                }
                return DorisConstant.createFloatConstant(globalState.getRandomly().getDouble());
            case DATE:
                if (!globalState.getDbmsSpecificOptions().testDateConstants) {
                    throw new IgnoreMeException();
                }
                timestamp = globalState.getRandomly().getLong(-62167420800000L, 253402185600000L);
                return DorisConstant.createDateConstant(timestamp);
            case DATETIME:
                // ['0000-01-01 00:00:00', '9999-12-31 23:59:59']
                if (!globalState.getDbmsSpecificOptions().testDateTimeConstants) {
                    throw new IgnoreMeException();
                }
                timestamp = globalState.getRandomly().getLong(-62167420800000L, 253402271999000L);
                return Randomly.fromOptions(DorisConstant.createDatetimeConstant(timestamp), DorisConstant.createDatetimeConstant());
            case VARCHAR:
                if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                    throw new IgnoreMeException();
                }
                return DorisConstant.createStringConstant(globalState.getRandomly().getString());
            case BOOLEAN:
                if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                    throw new IgnoreMeException();
                }
                return DorisConstant.createBooleanConstant(Randomly.getBoolean());
            default:
                System.out.println("ERROR TYPE:" + columnDataType);
                throw new AssertionError();
        }
    }

    @Override
    public List<Node<DorisExpression>> generateOrderBys() {
        List<Node<DorisExpression>> expr = super.generateOrderBys();
        List<Node<DorisExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<DorisExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public static class DorisCastOperation extends NewUnaryPostfixOperatorNode<DorisExpression> {

        public DorisCastOperation(Node<DorisExpression> expr, DorisCompositeDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    public enum DorisAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), STRING_AGG(1), FIRST(1), SUM(1), STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private int nrArgs;

        DorisAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static DorisAggregateFunction getRandom() {
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
        // LEFT(2), https://github.com/cwida/Doris/issues/633
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

    public enum DorisUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        DorisUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DorisUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DorisUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        DorisUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DorisUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DorisBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DorisBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), LSHIFT("<<"), RSHIFT(">>");

        private String textRepr;

        DorisBinaryArithmeticOperator(String textRepr) {
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

    public enum DorisBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE"), SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO"),
        REGEX_POSIX("~"), REGEX_POSIT_NOT("!~");

        private String textRepr;

        DorisBinaryComparisonOperator(String textRepr) {
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

    public NewFunctionNode<DorisExpression, DorisAggregateFunction> generateArgsForAggregate(
            DorisAggregateFunction aggregateFunction) {
        return new NewFunctionNode<DorisExpression, DorisExpressionGenerator.DorisAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<DorisExpression> generateAggregate() {
        DorisAggregateFunction aggrFunc = DorisAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<DorisExpression> negatePredicate(Node<DorisExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, DorisUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<DorisExpression> isNull(Node<DorisExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, DorisUnaryPostfixOperator.IS_NULL);
    }

}
