package sqlancer.duckdb.gen;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBBetweenOperator;
import sqlancer.duckdb.ast.DuckDBBinaryOperator;
import sqlancer.duckdb.ast.DuckDBCaseOperator;
import sqlancer.duckdb.ast.DuckDBColumnReference;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBFunction;
import sqlancer.duckdb.ast.DuckDBInOperator;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBOrderingTerm;
import sqlancer.duckdb.ast.DuckDBPostFixText;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTableReference;
import sqlancer.duckdb.ast.DuckDBTernary;

public final class DuckDBExpressionGenerator extends UntypedExpressionGenerator<DuckDBExpression, DuckDBColumn>
        implements NoRECGenerator<DuckDBSelect, DuckDBJoin, DuckDBExpression, DuckDBTable, DuckDBColumn>,
        TLPWhereGenerator<DuckDBSelect, DuckDBJoin, DuckDBExpression, DuckDBTable, DuckDBColumn> {

    private final DuckDBGlobalState globalState;
    private List<DuckDBTable> tables;

    public DuckDBExpressionGenerator(DuckDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, CASE,
        IN, COLLATE, LIKE_ESCAPE
    }

    @Override
    protected DuckDBExpression generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            DuckDBAggregateFunction aggregate = DuckDBAggregateFunction.getRandom();
            allowAggregates = false;
            return new DuckDBFunction<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
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
            return new sqlancer.duckdb.ast.DuckDBUnaryPostfixOperator(generateExpression(depth + 1),
                    DuckDBCollate.getRandom());
        case UNARY_PREFIX:
            return new sqlancer.duckdb.ast.DuckDBUnaryPrefixOperator(generateExpression(depth + 1),
                    DuckDBUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new sqlancer.duckdb.ast.DuckDBUnaryPostfixOperator(generateExpression(depth + 1),
                    DuckDBUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = DuckDBBinaryComparisonOperator.getRandom();
            return new DuckDBBinaryOperator(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = DuckDBBinaryLogicalOperator.getRandom();
            return new DuckDBBinaryOperator(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new DuckDBBinaryOperator(generateExpression(depth + 1), generateExpression(depth + 1),
                    DuckDBBinaryArithmeticOperator.getRandom());
        case CAST:
            return new DuckDBCastOperation(generateExpression(depth + 1),
                    DuckDBCompositeDataType.getRandomWithoutNull());
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new DuckDBFunction<>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new DuckDBBetweenOperator(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new DuckDBInOperator(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new DuckDBCaseOperator(generateExpression(depth + 1), generateExpressions(nr, depth + 1),
                    generateExpressions(nr, depth + 1), generateExpression(depth + 1));
        case LIKE_ESCAPE:
            return new DuckDBTernary(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), "LIKE", "ESCAPE");
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected DuckDBExpression generateColumn() {
        DuckDBColumn column = Randomly.fromList(columns);
        return new DuckDBColumnReference(column);
    }

    @Override
    public DuckDBExpression generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return DuckDBConstant.createNullConstant();
        }
        DuckDBDataType type = DuckDBDataType.getRandomWithoutNull();
        switch (type) {
        case INT:
            if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createIntConstant(BigInteger.valueOf(globalState.getRandomly().getInteger()));
        case DATE:
            if (!globalState.getDbmsSpecificOptions().testDateConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createDateConstant(globalState.getRandomly().getInteger());
        case TIMESTAMP:
            if (!globalState.getDbmsSpecificOptions().testTimestampConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public List<DuckDBExpression> generateOrderBys() {
        List<DuckDBExpression> expr = super.generateOrderBys();
        List<DuckDBExpression> newExpr = new ArrayList<>(expr.size());
        for (DuckDBExpression curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new DuckDBOrderingTerm(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    @Override
    public List<DuckDBExpression> generateGroupBys() {
        List<DuckDBExpression> expr = super.generateGroupBys();
        return expr;
    };

    public static class DuckDBCastOperation extends NewUnaryPostfixOperatorNode<DuckDBExpression>
            implements DuckDBExpression {

        public DuckDBCastOperation(DuckDBExpression expr, DuckDBCompositeDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    public enum DuckDBAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), STRING_AGG(1), FIRST(1), SUM(1), STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private int nrArgs;

        DuckDBAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static DuckDBAggregateFunction getRandom() {
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
        // LEFT(2), https://github.com/cwida/duckdb/issues/633
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

    public enum DuckDBUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        DuckDBUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DuckDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static final class DuckDBCollate implements Operator {

        private final String textRepr;

        private DuckDBCollate(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return "COLLATE " + textRepr;
        }

        public static DuckDBCollate getRandom() {
            return new DuckDBCollate(DuckDBTableGenerator.getRandomCollate());
        }

    }

    public enum DuckDBUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        DuckDBUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DuckDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DuckDBBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DuckDBBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), LSHIFT("<<"), RSHIFT(">>");

        private String textRepr;

        DuckDBBinaryArithmeticOperator(String textRepr) {
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

    public enum DuckDBBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE"), SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO"),
        REGEX_POSIX("~"), REGEX_POSIT_NOT("!~");

        private String textRepr;

        DuckDBBinaryComparisonOperator(String textRepr) {
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

    public DuckDBFunction<DuckDBAggregateFunction> generateArgsForAggregate(DuckDBAggregateFunction aggregateFunction) {
        return new DuckDBFunction<>(generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public DuckDBExpression generateAggregate() {
        DuckDBAggregateFunction aggrFunc = DuckDBAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public DuckDBExpression negatePredicate(DuckDBExpression predicate) {
        return new sqlancer.duckdb.ast.DuckDBUnaryPrefixOperator(predicate, DuckDBUnaryPrefixOperator.NOT);
    }

    @Override
    public DuckDBExpression isNull(DuckDBExpression expr) {
        return new sqlancer.duckdb.ast.DuckDBUnaryPostfixOperator(expr, DuckDBUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public DuckDBExpressionGenerator setTablesAndColumns(AbstractTables<DuckDBTable, DuckDBColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public DuckDBExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public DuckDBSelect generateSelect() {
        return new DuckDBSelect();
    }

    @Override
    public List<DuckDBJoin> getRandomJoinClauses() {
        List<DuckDBTableReference> tableList = tables.stream().map(t -> new DuckDBTableReference(t))
                .collect(Collectors.toList());
        List<DuckDBJoin> joins = DuckDBJoin.getJoins(tableList, globalState);
        tables = tableList.stream().map(t -> t.getTable()).collect(Collectors.toList());
        return joins;
    }

    @Override
    public List<DuckDBExpression> getTableRefs() {
        return tables.stream().map(t -> new DuckDBTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public String generateOptimizedQueryString(DuckDBSelect select, DuckDBExpression whereCondition,
            boolean shouldUseAggregate) {
        List<DuckDBExpression> allColumns = columns.stream().map((c) -> new DuckDBColumnReference(c))
                .collect(Collectors.toList());
        if (shouldUseAggregate) {
            DuckDBFunction<DuckDBAggregateFunction> aggr = new DuckDBFunction<>(
                    Arrays.asList(new DuckDBColumnReference(
                            new DuckDBColumn("*", new DuckDBCompositeDataType(DuckDBDataType.INT, 0), false, false))),
                    DuckDBAggregateFunction.COUNT);
            select.setFetchColumns(Arrays.asList(aggr));
        } else {
            select.setFetchColumns(allColumns);
            if (Randomly.getBooleanWithSmallProbability()) {
                select.setOrderByClauses(generateOrderBys());
            }
        }
        select.setWhereClause(whereCondition);

        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(DuckDBSelect select, DuckDBExpression whereCondition) {
        DuckDBExpression asText = new DuckDBPostFixText(new DuckDBCastOperation(
                new DuckDBPostFixText(whereCondition,
                        " IS NOT NULL AND " + DuckDBToStringVisitor.asString(whereCondition)),
                new DuckDBCompositeDataType(DuckDBDataType.INT, 8)), "as count");
        select.setFetchColumns(Arrays.asList(asText));

        return "SELECT SUM(count) FROM (" + select.asString() + ") as res";
    }

    @Override
    public List<DuckDBExpression> generateFetchColumns(boolean shouldCreateDummy) {
        if (Randomly.getBoolean()) {
            return List.of(new DuckDBColumnReference(new DuckDBColumn("*", null, false, false)));
        }
        return Randomly.nonEmptySubset(columns).stream().map(c -> new DuckDBColumnReference(c))
                .collect(Collectors.toList());
    }
}
