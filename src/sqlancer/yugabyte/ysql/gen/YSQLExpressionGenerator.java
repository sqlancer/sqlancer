package sqlancer.yugabyte.ysql.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.yugabyte.ysql.YSQLCompoundDataType;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLProvider;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLRowValue;
import sqlancer.yugabyte.ysql.ast.YSQLAggregate;
import sqlancer.yugabyte.ysql.ast.YSQLBetweenOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryArithmeticOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryBitOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryComparisonOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryLogicalOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryRangeOperation;
import sqlancer.yugabyte.ysql.ast.YSQLCaseExpression;
import sqlancer.yugabyte.ysql.ast.YSQLCastOperation;
import sqlancer.yugabyte.ysql.ast.YSQLJSONBOperation;
import sqlancer.yugabyte.ysql.ast.YSQLJSONBFunction;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLConcatOperation;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLFunction;
import sqlancer.yugabyte.ysql.ast.YSQLFunctionWithUnknownResult;
import sqlancer.yugabyte.ysql.ast.YSQLInOperation;
import sqlancer.yugabyte.ysql.ast.YSQLOrderByTerm;
import sqlancer.yugabyte.ysql.ast.YSQLPOSIXRegularExpression;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLPrefixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLSimilarTo;

public class YSQLExpressionGenerator implements ExpressionGenerator<YSQLExpression> {

    private final int maxDepth;

    private final Randomly r;
    private final Map<String, Character> functionsAndTypes;
    private final List<Character> allowedFunctionTypes;
    private List<YSQLColumn> columns;
    private YSQLRowValue rw;
    private boolean expectedResult;
    public YSQLGlobalState globalState;
    private boolean allowAggregateFunctions;

    public YSQLExpressionGenerator(YSQLGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
        this.functionsAndTypes = globalState.getFunctionsAndTypes();
        this.allowedFunctionTypes = globalState.getAllowedFunctionTypes();
    }

    public static YSQLExpression generateExpression(YSQLGlobalState globalState, YSQLDataType type) {
        return new YSQLExpressionGenerator(globalState).generateExpression(0, type);
    }

    private static YSQLCompoundDataType getCompoundDataType(YSQLDataType type) {
        switch (type) {
        case BOOLEAN:
        case SMALLINT:
        case INT:
        case BIGINT:
        case NUMERIC:
        case DECIMAL:
        case FLOAT:
        case REAL:
        case DOUBLE_PRECISION:
        case MONEY:
        case RANGE:
        case INT4RANGE:
        case INT8RANGE:
        case NUMRANGE:
        case TSRANGE:
        case TSTZRANGE:
        case DATERANGE:
        case INET:
        case CIDR:
        case MACADDR:
        case BYTEA:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMPTZ:
        case INTERVAL:
        case UUID:
        case JSON:
        case JSONB:
        case POINT:
        case LINE:
        case LSEG:
        case BOX:
        case PATH:
        case POLYGON:
        case CIRCLE:
        case INT_ARRAY:
        case TEXT_ARRAY:
        case BOOLEAN_ARRAY:
            return YSQLCompoundDataType.create(type);
        case TEXT:
            // TEXT type does not accept size modifier
            return YSQLCompoundDataType.create(type);
        case VARCHAR:
        case CHAR:
        case BIT:
            if (Randomly.getBoolean()
                    || YSQLProvider.generateOnlyKnown /*
                                                       * The PQS implementation does not check for size specifications
                                                       */) {
                return YSQLCompoundDataType.create(type);
            } else {
                return YSQLCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
            }
        default:
            throw new AssertionError(type);
        }

    }

    public static YSQLExpression generateConstant(Randomly r, YSQLDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return YSQLConstant.createNullConstant();
        }
        // if (Randomly.getBooleanWithSmallProbability()) {
        // return YSQLConstant.createTextConstant(r.getString());
        // }
        switch (type) {
        case SMALLINT:
            return YSQLConstant.createIntConstant(r.getInteger(-32768, 32767));
        case INT:
            if (Randomly.getBooleanWithSmallProbability()) {
                int validInt = r.getInteger(Integer.MIN_VALUE, Integer.MAX_VALUE);
                return YSQLConstant.createTextConstant(String.valueOf(validInt));
            } else {
                if (r.getInteger(0, 10) < 8) {
                    return YSQLConstant.createIntConstant(r.getInteger(0, 10000));
                } else {
                    return YSQLConstant.createIntConstant(r.getInteger(Integer.MIN_VALUE, Integer.MAX_VALUE));
                }
            }
        case BIGINT:
            return YSQLConstant.createIntConstant(r.getLong(Long.MIN_VALUE, Long.MAX_VALUE));
        case BOOLEAN:
            if (Randomly.getBooleanWithSmallProbability() && !YSQLProvider.generateOnlyKnown) {
                return YSQLConstant
                        .createTextConstant(Randomly.fromOptions("TR", "TRUE", "FA", "FALSE", "0", "1", "ON", "off"));
            } else {
                return YSQLConstant.createBooleanConstant(Randomly.getBoolean());
            }
        case VARCHAR:
        case CHAR:
        case TEXT:
            return YSQLConstant.createTextConstant(r.getString());
        case NUMERIC:
        case DECIMAL:
            return YSQLConstant.createDecimalConstant(r.getRandomBigDecimal());
        case FLOAT:
            return YSQLConstant.createFloatConstant((float) r.getDouble());
        case REAL:
            return YSQLConstant.createFloatConstant((float) r.getDouble());
        case DOUBLE_PRECISION:
            return YSQLConstant.createDoubleConstant(r.getDouble());
        case DATE:
            // Generate dates between 1900 and 2100
            return YSQLConstant.createTextConstant(String.format("%04d-%02d-%02d", 
                r.getInteger(1900, 2100), r.getInteger(1, 12), r.getInteger(1, 28)));
        case TIME:
            return YSQLConstant.createTextConstant(String.format("%02d:%02d:%02d",
                r.getInteger(0, 23), r.getInteger(0, 59), r.getInteger(0, 59)));
        case TIMESTAMP:
        case TIMESTAMPTZ:
            return YSQLConstant.createTextConstant(String.format("%04d-%02d-%02d %02d:%02d:%02d",
                r.getInteger(1900, 2100), r.getInteger(1, 12), r.getInteger(1, 28),
                r.getInteger(0, 23), r.getInteger(0, 59), r.getInteger(0, 59)));
        case UUID:
            return YSQLConstant.createTextConstant(java.util.UUID.randomUUID().toString());
        case JSON:
        case JSONB:
            // Simple JSON generation
            return YSQLConstant.createTextConstant(
                Randomly.fromOptions(
                    "{\"key\": \"value\"}",
                    "[1, 2, 3]",
                    "{\"a\": 1, \"b\": \"text\"}",
                    "null",
                    "true",
                    "42"
                ));
        case RANGE:
        case INT4RANGE:
        case INT8RANGE:
            return YSQLConstant.createRange(r.getInteger(), Randomly.getBoolean(), r.getInteger(),
                    Randomly.getBoolean());
        case MONEY:
            return new YSQLCastOperation(generateConstant(r, YSQLDataType.FLOAT),
                    getCompoundDataType(YSQLDataType.MONEY));
        case INET:
            return YSQLConstant.createInetConstant(getRandomInet(r));
        case CIDR:
            // Generate valid CIDR with proper masking
            int maskBits = r.getInteger(0, 33); // 0-32 inclusive
            if (maskBits == 0) {
                return YSQLConstant.createTextConstant("0.0.0.0/0");
            }
            long ip = r.getInteger(0, Integer.MAX_VALUE) & 0xFFFFFFFFL;
            // Zero out host bits
            int hostBits = 32 - maskBits;
            if (hostBits > 0) {
                long mask = ~((1L << hostBits) - 1);
                ip = ip & mask;
            }
            String ipStr = String.format("%d.%d.%d.%d",
                    (ip >> 24) & 0xFF,
                    (ip >> 16) & 0xFF,
                    (ip >> 8) & 0xFF,
                    ip & 0xFF);
            return YSQLConstant.createTextConstant(ipStr + "/" + maskBits);
        case MACADDR:
            return YSQLConstant.createTextConstant(String.format("%02x:%02x:%02x:%02x:%02x:%02x",
                r.getInteger(0, 255), r.getInteger(0, 255), r.getInteger(0, 255),
                r.getInteger(0, 255), r.getInteger(0, 255), r.getInteger(0, 255)));
        case BIT:
            // Generate bit constants with consistent sizes to avoid XOR errors
            int bitSize = r.getInteger(1, 32);
            int maxValue = (1 << bitSize) - 1;
            return YSQLConstant.createBitConstant(r.getInteger(0, maxValue));
        case BYTEA:
            return YSQLConstant.createByteConstant(String.valueOf(r.getInteger()));
        case INTERVAL:
            // Generate simple intervals
            return YSQLConstant.createTextConstant(
                r.getInteger(1, 100) + " " + 
                Randomly.fromOptions("days", "hours", "minutes", "seconds", "months", "years"));
        case INT_ARRAY:
            // Generate simple integer arrays
            return YSQLConstant.createTextConstant(
                "{" + r.getInteger() + "," + r.getInteger() + "," + r.getInteger() + "}");
        case TEXT_ARRAY:
            return YSQLConstant.createTextConstant(
                "{\"" + r.getString() + "\",\"" + r.getString() + "\"}");
        case BOOLEAN_ARRAY:
            return YSQLConstant.createTextConstant(
                "{" + Randomly.fromOptions("true", "false") + "," + 
                Randomly.fromOptions("true", "false") + "}");
        case NUMRANGE:
        case TSRANGE:
        case TSTZRANGE:
        case DATERANGE:
            // For these range types, generate as text for now
            return YSQLConstant.createTextConstant("[0,100]");
        case POINT:
            return YSQLConstant.createTextConstant(
                "(" + (r.getDouble() * 360 - 180) + "," + (r.getDouble() * 180 - 90) + ")");
        case LINE:
            return YSQLConstant.createTextConstant(
                "{" + r.getDouble() + "," + r.getDouble() + "," + r.getDouble() + "}");
        case LSEG:
            return YSQLConstant.createTextConstant(
                "[(" + r.getDouble() + "," + r.getDouble() + ")," +
                "(" + r.getDouble() + "," + r.getDouble() + ")]");
        case BOX:
            double x1 = r.getDouble();
            double y1 = r.getDouble();
            double x2 = r.getDouble();
            double y2 = r.getDouble();
            return YSQLConstant.createTextConstant(
                "((" + Math.max(x1, x2) + "," + Math.max(y1, y2) + ")," +
                "(" + Math.min(x1, x2) + "," + Math.min(y1, y2) + "))");
        case PATH:
            return YSQLConstant.createTextConstant(
                "[(" + r.getDouble() + "," + r.getDouble() + ")," +
                "(" + r.getDouble() + "," + r.getDouble() + ")," +
                "(" + r.getDouble() + "," + r.getDouble() + ")]");
        case POLYGON:
            return YSQLConstant.createTextConstant(
                "((" + r.getDouble() + "," + r.getDouble() + ")," +
                "(" + r.getDouble() + "," + r.getDouble() + ")," +
                "(" + r.getDouble() + "," + r.getDouble() + "))");
        case CIRCLE:
            return YSQLConstant.createTextConstant(
                "<(" + r.getDouble() + "," + r.getDouble() + ")," + (r.getDouble() * 99.9 + 0.1) + ">");
        default:
            throw new AssertionError(type);
        }
    }

    private static String getRandomInet(Randomly r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i != 0) {
                sb.append('.');
            }
            sb.append(r.getInteger() & 255);
        }
        return sb.toString();
    }

    public static YSQLExpression generateExpression(YSQLGlobalState globalState, List<YSQLColumn> columns,
            YSQLDataType type) {
        return new YSQLExpressionGenerator(globalState).setColumns(columns).generateExpression(0, type);
    }

    public static YSQLExpression generateExpression(YSQLGlobalState globalState, List<YSQLColumn> columns) {
        return new YSQLExpressionGenerator(globalState).setColumns(columns).generateExpression(0);

    }

    public YSQLExpressionGenerator setColumns(List<YSQLColumn> columns) {
        this.columns = columns;
        return this;
    }

    public YSQLExpressionGenerator setRowValue(YSQLRowValue rw) {
        this.rw = rw;
        return this;
    }

    public YSQLExpression generateExpression(int depth) {
        return generateExpression(depth, YSQLDataType.getRandomType());
    }

    public List<YSQLExpression> generateOrderBy() {
        List<YSQLExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new YSQLOrderByTerm(YSQLColumnValue.create(Randomly.fromList(columns), null),
                    YSQLOrderByTerm.YSQLOrder.getRandomOrder()));
        }
        return orderBys;
    }

    private YSQLExpression generateFunctionWithUnknownResult(int depth, YSQLDataType type) {
        List<YSQLFunctionWithUnknownResult> supportedFunctions = YSQLFunctionWithUnknownResult
                .getSupportedFunctions(type);
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        supportedFunctions = supportedFunctions.stream()
                .filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (supportedFunctions.isEmpty()) {
            throw new IgnoreMeException();
        }
        YSQLFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
        return new YSQLFunction(randomFunction, type, randomFunction.getArguments(type, this, depth + 1));
    }

    private YSQLExpression generateFunctionWithKnownResult(int depth, YSQLDataType type) {
        List<YSQLFunction.YSQLFunctionWithResult> functions = Stream.of(YSQLFunction.YSQLFunctionWithResult.values())
                .filter(f -> f.supportsReturnType(type)).collect(Collectors.toList());
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        functions = functions.stream().filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (functions.isEmpty()) {
            throw new IgnoreMeException();
        }
        YSQLFunction.YSQLFunctionWithResult randomFunction = Randomly.fromList(functions);
        int nrArgs = randomFunction.getNrArgs();
        if (randomFunction.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        YSQLDataType[] argTypes = randomFunction.getInputTypesForReturnType(type, nrArgs);
        YSQLExpression[] args = new YSQLExpression[nrArgs];
        do {
            for (int i = 0; i < args.length; i++) {
                args[i] = generateExpression(depth + 1, argTypes[i]);
            }
        } while (!randomFunction.checkArguments(args));
        return new YSQLFunction(randomFunction, type, args);
    }

    private YSQLExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        if (YSQLProvider.generateOnlyKnown) {
            validOptions.remove(BooleanExpression.SIMILAR_TO);
            validOptions.remove(BooleanExpression.POSIX_REGEX);
            validOptions.remove(BooleanExpression.BINARY_RANGE_COMPARISON);
        }
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case POSTFIX_OPERATOR:
            YSQLPostfixOperation.PostfixOperator random = YSQLPostfixOperation.PostfixOperator.getRandom();
            return YSQLPostfixOperation
                    .create(generateExpression(depth + 1, Randomly.fromOptions(random.getInputDataTypes())), random);
        case IN_OPERATION:
            return inOperation(depth + 1);
        case NOT:
            return new YSQLPrefixOperation(generateExpression(depth + 1, YSQLDataType.BOOLEAN),
                    YSQLPrefixOperation.PrefixOperator.NOT);
        case BINARY_LOGICAL_OPERATOR:
            YSQLExpression first = generateExpression(depth + 1, YSQLDataType.BOOLEAN);
            int nr = Randomly.smallNumber() + 1;
            for (int i = 0; i < nr; i++) {
                first = new YSQLBinaryLogicalOperation(first, generateExpression(depth + 1, YSQLDataType.BOOLEAN),
                        YSQLBinaryLogicalOperation.BinaryLogicalOperator.getRandom());
            }
            return first;
        case BINARY_COMPARISON:
            YSQLDataType dataType = getComparisonSafeType();
            return generateComparison(depth, dataType);
        case CAST:
            return new YSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(YSQLDataType.BOOLEAN));
        case FUNCTION:
            return generateFunction(depth + 1, YSQLDataType.BOOLEAN);
        case BETWEEN:
            YSQLDataType type = getComparisonSafeType();
            return new YSQLBetweenOperation(generateExpression(depth + 1, type), generateExpression(depth + 1, type),
                    generateExpression(depth + 1, type), Randomly.getBoolean());
        case SIMILAR_TO:
            assert !expectedResult;
            return new YSQLSimilarTo(generateExpression(depth + 1, YSQLDataType.TEXT),
                    YSQLConstant.createTextConstant(Randomly.fromOptions("test", "[a-z]+", ".*", "[0-9]*", "abc")), null);
        case POSIX_REGEX:
            assert !expectedResult;
            YSQLExpression text = generateExpression(depth + 1, YSQLDataType.TEXT);
            YSQLExpression regex = YSQLConstant.createTextConstant(Randomly.fromOptions("test", "[a-z]+", ".*", "[0-9]*", "abc"));
            return new YSQLPOSIXRegularExpression(text, regex,
                    YSQLPOSIXRegularExpression.POSIXRegex.getRandom());
        case BINARY_RANGE_COMPARISON:
            // TODO element check
            return new YSQLBinaryRangeOperation(YSQLBinaryRangeOperation.YSQLBinaryRangeComparisonOperator.getRandom(),
                    generateExpression(depth + 1, YSQLDataType.RANGE),
                    generateExpression(depth + 1, YSQLDataType.RANGE));
        case CASE_EXPRESSION:
            return generateCaseExpression(depth + 1, YSQLDataType.BOOLEAN);
        default:
            throw new AssertionError();
        }
    }

    private YSQLDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return YSQLDataType.getRandomType();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }
    
    private YSQLDataType getComparisonSafeType() {
        YSQLDataType[] comparisonSafeTypes = {
            YSQLDataType.SMALLINT, YSQLDataType.INT, YSQLDataType.BIGINT,
            YSQLDataType.NUMERIC, YSQLDataType.DECIMAL, YSQLDataType.REAL, 
            YSQLDataType.DOUBLE_PRECISION, YSQLDataType.FLOAT,
            YSQLDataType.VARCHAR, YSQLDataType.CHAR, YSQLDataType.TEXT,
            YSQLDataType.DATE, YSQLDataType.TIME, YSQLDataType.TIMESTAMP, 
            YSQLDataType.TIMESTAMPTZ, YSQLDataType.INTERVAL,
            YSQLDataType.BOOLEAN, YSQLDataType.MONEY
        };
        
        if (columns != null && !columns.isEmpty() && Randomly.getBoolean()) {
            YSQLDataType columnType = Randomly.fromList(columns).getType();
            for (YSQLDataType safeType : comparisonSafeTypes) {
                if (columnType == safeType) {
                    return columnType;
                }
            }
        }
        
        return Randomly.fromOptions(comparisonSafeTypes);
    }

    private YSQLExpression generateFunction(int depth, YSQLDataType type) {
        if (YSQLProvider.generateOnlyKnown || Randomly.getBoolean()) {
            return generateFunctionWithKnownResult(depth, type);
        } else {
            return generateFunctionWithUnknownResult(depth, type);
        }
    }

    private YSQLExpression generateComparison(int depth, YSQLDataType dataType) {
        YSQLExpression leftExpr = generateExpression(depth + 1, dataType);
        YSQLExpression rightExpr = generateExpression(depth + 1, dataType);
        return getComparison(leftExpr, rightExpr);
    }

    private YSQLExpression getComparison(YSQLExpression leftExpr, YSQLExpression rightExpr) {
        return new YSQLBinaryComparisonOperation(leftExpr, rightExpr,
                YSQLBinaryComparisonOperation.YSQLBinaryComparisonOperator.getRandom());
    }

    private YSQLExpression inOperation(int depth) {
        YSQLDataType type = YSQLDataType.getRandomType();
        YSQLExpression leftExpr = generateExpression(depth + 1, type);
        List<YSQLExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateExpression(depth + 1, type));
        }
        return new YSQLInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }
    
    private YSQLExpression generateCaseExpression(int depth, YSQLDataType resultType) {
        int numCases = Randomly.smallNumber() + 1;
        List<YSQLExpression> conditions = new ArrayList<>();
        List<YSQLExpression> results = new ArrayList<>();
        
        if (Randomly.getBoolean()) {
            // Simple CASE
            YSQLDataType switchType = getMeaningfulType();
            YSQLExpression switchExpr = generateExpression(depth + 1, switchType);
            
            for (int i = 0; i < numCases; i++) {
                conditions.add(generateExpression(depth + 1, switchType));
                results.add(generateExpression(depth + 1, resultType));
            }
            
            YSQLExpression elseResult = Randomly.getBoolean() ? generateExpression(depth + 1, resultType) : null;
            return YSQLCaseExpression.createSimpleCase(switchExpr, conditions, results, elseResult);
        } else {
            // Searched CASE
            for (int i = 0; i < numCases; i++) {
                conditions.add(generateExpression(depth + 1, YSQLDataType.BOOLEAN));
                results.add(generateExpression(depth + 1, resultType));
            }
            
            YSQLExpression elseResult = Randomly.getBoolean() ? generateExpression(depth + 1, resultType) : null;
            return YSQLCaseExpression.createSearchedCase(conditions, results, elseResult);
        }
    }

    public YSQLExpression generateExpression(int depth, YSQLDataType originalType) {
        YSQLDataType dataType = originalType;
        if (dataType == YSQLDataType.REAL && Randomly.getBoolean()) {
            dataType = Randomly.fromOptions(YSQLDataType.INT, YSQLDataType.FLOAT);
        }
        if (dataType == YSQLDataType.FLOAT && Randomly.getBoolean()) {
            dataType = YSQLDataType.INT;
        }
        return generateExpressionInternal(depth, dataType);
    }

    private YSQLExpression generateExpressionInternal(int depth, YSQLDataType dataType) throws AssertionError {
        if (allowAggregateFunctions && Randomly.getBoolean()) {
            allowAggregateFunctions = false; // aggregate function calls cannot be nested
            return getAggregate(dataType);
        }
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            // generic expression
            if (Randomly.getBoolean() || depth > maxDepth) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    return generateConstant(r, dataType);
                } else {
                    if (filterColumns(dataType).isEmpty()) {
                        return generateConstant(r, dataType);
                    } else {
                        return createColumnOfType(dataType);
                    }
                }
            } else {
                // Don't generate CAST operations for types that can't be cast from arbitrary types
                boolean isSpecialType = dataType == YSQLDataType.RANGE || 
                                       dataType == YSQLDataType.INT4RANGE || 
                                       dataType == YSQLDataType.INT8RANGE ||
                                       dataType == YSQLDataType.NUMRANGE || 
                                       dataType == YSQLDataType.TSRANGE ||
                                       dataType == YSQLDataType.TSTZRANGE || 
                                       dataType == YSQLDataType.DATERANGE ||
                                       dataType == YSQLDataType.CIDR ||
                                       dataType == YSQLDataType.INET ||
                                       dataType == YSQLDataType.MACADDR ||
                                       dataType == YSQLDataType.UUID ||
                                       dataType == YSQLDataType.POINT ||
                                       dataType == YSQLDataType.LINE ||
                                       dataType == YSQLDataType.LSEG ||
                                       dataType == YSQLDataType.BOX ||
                                       dataType == YSQLDataType.PATH ||
                                       dataType == YSQLDataType.POLYGON ||
                                       dataType == YSQLDataType.CIRCLE;
                
                if (Randomly.getBoolean() && !isSpecialType) {
                    // For BIT type, only cast from compatible types
                    if (dataType == YSQLDataType.BIT) {
                        YSQLDataType sourceType = Randomly.fromOptions(
                            YSQLDataType.INT, YSQLDataType.BIGINT, YSQLDataType.SMALLINT,
                            YSQLDataType.BIT, YSQLDataType.VARCHAR, YSQLDataType.TEXT
                        );
                        return new YSQLCastOperation(generateExpression(depth + 1, sourceType), getCompoundDataType(dataType));
                    } else {
                        return new YSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
                    }
                } else {
                    return generateFunctionWithUnknownResult(depth, dataType);
                }
            }
        } else {
            switch (dataType) {
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case SMALLINT:
            case INT:
            case BIGINT:
                return generateIntExpression(depth);
            case VARCHAR:
            case CHAR:
            case TEXT:
                return generateTextExpression(depth);
            case NUMERIC:
            case DECIMAL:
            case REAL:
            case DOUBLE_PRECISION:
            case FLOAT:
            case MONEY:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case INTERVAL:
            case INET:
            case CIDR:
            case MACADDR:
            case UUID:
            case JSON:
                return generateConstant(r, dataType);
            case JSONB:
                return generateJSONBExpression(depth);
            case INT_ARRAY:
            case TEXT_ARRAY:
            case BOOLEAN_ARRAY:
                return generateConstant(r, dataType);
            case BYTEA:
                return generateByteExpression();
            case BIT:
                return generateBitExpression(depth);
            case RANGE:
            case INT4RANGE:
            case INT8RANGE:
            case NUMRANGE:
            case TSRANGE:
            case TSTZRANGE:
            case DATERANGE:
                return generateRangeExpression(depth);
            case POINT:
            case LINE:
            case LSEG:
            case BOX:
            case PATH:
            case POLYGON:
            case CIRCLE:
                // For now, treat geometric types as constants
                return generateConstant(r, dataType);
            default:
                throw new AssertionError(dataType);
            }
        }
    }

    private YSQLExpression generateRangeExpression(int depth) {
        RangeExpression option;
        List<RangeExpression> validOptions = new ArrayList<>(Arrays.asList(RangeExpression.values()));
        option = Randomly.fromList(validOptions);
        switch (option) {
        case BINARY_OP:
            return new YSQLBinaryRangeOperation(YSQLBinaryRangeOperation.YSQLBinaryRangeOperator.getRandom(),
                    generateExpression(depth + 1, YSQLDataType.RANGE),
                    generateExpression(depth + 1, YSQLDataType.RANGE));
        default:
            throw new AssertionError(option);
        }
    }

    private YSQLExpression generateTextExpression(int depth) {
        TextExpression option;
        List<TextExpression> validOptions = new ArrayList<>(Arrays.asList(TextExpression.values()));
        option = Randomly.fromList(validOptions);

        switch (option) {
        case CAST:
            // Avoid casting bit operations to text as it can cause "invalid binary digit" errors
            YSQLDataType sourceType = Randomly.fromOptions(
                YSQLDataType.INT, YSQLDataType.BIGINT, YSQLDataType.SMALLINT,
                YSQLDataType.FLOAT, YSQLDataType.REAL, YSQLDataType.DOUBLE_PRECISION,
                YSQLDataType.NUMERIC, YSQLDataType.DECIMAL, YSQLDataType.BOOLEAN,
                YSQLDataType.DATE, YSQLDataType.TIME, YSQLDataType.TIMESTAMP,
                YSQLDataType.INTERVAL, YSQLDataType.MONEY, YSQLDataType.INET,
                YSQLDataType.CIDR, YSQLDataType.VARCHAR, YSQLDataType.CHAR
            );
            return new YSQLCastOperation(generateExpression(depth + 1, sourceType), getCompoundDataType(YSQLDataType.TEXT));
        case FUNCTION:
            return generateFunction(depth + 1, YSQLDataType.TEXT);
        case CONCAT:
            return generateConcat(depth);
        case CASE_EXPRESSION:
            return generateCaseExpression(depth + 1, YSQLDataType.TEXT);
        default:
            throw new AssertionError();
        }
    }

    private YSQLExpression generateConcat(int depth) {
        YSQLExpression left = generateExpression(depth + 1, YSQLDataType.TEXT);
        YSQLExpression right = generateExpression(depth + 1);
        return new YSQLConcatOperation(left, right);
    }

    private YSQLExpression generateByteExpression() {
        // Generate random bytea values
        if (Randomly.getBoolean()) {
            // Generate hex-encoded bytea
            StringBuilder hex = new StringBuilder("\\x");
            int length = r.getInteger(1, 20);
            for (int i = 0; i < length; i++) {
                hex.append(String.format("%02x", r.getInteger(0, 255)));
            }
            return YSQLConstant.createByteConstant(hex.toString());
        } else {
            // Generate escape-encoded bytea
            StringBuilder escaped = new StringBuilder();
            int length = r.getInteger(1, 20);
            for (int i = 0; i < length; i++) {
                int b = r.getInteger(0, 255);
                if (b == 0) {
                    escaped.append("\\000");
                } else if (b == 39) { // single quote
                    escaped.append("''");
                } else if (b == 92) { // backslash
                    escaped.append("\\\\");
                } else if (b < 32 || b > 126) {
                    escaped.append(String.format("\\%03o", b));
                } else {
                    escaped.append((char) b);
                }
            }
            return YSQLConstant.createByteConstant(escaped.toString());
        }
    }
    
    private enum JSONBExpression {
        CONSTANT, OPERATOR, FUNCTION, CAST, BUILD_FUNCTION
    }
    
    private YSQLExpression generateJSONBExpression(int depth) {
        if (depth >= maxDepth) {
            return generateConstant(r, YSQLDataType.JSONB);
        }
        
        JSONBExpression option = Randomly.fromOptions(JSONBExpression.values());
        switch (option) {
        case CONSTANT:
            return generateConstant(r, YSQLDataType.JSONB);
        case OPERATOR:
            return generateJSONBOperator(depth);
        case FUNCTION:
            return generateJSONBFunction(depth);
        case CAST:
            return new YSQLCastOperation(generateExpression(depth + 1, YSQLDataType.TEXT), 
                getCompoundDataType(YSQLDataType.JSONB));
        case BUILD_FUNCTION:
            return generateJSONBBuildFunction(depth);
        default:
            throw new AssertionError(option);
        }
    }
    
    private YSQLExpression generateJSONBOperator(int depth) {
        YSQLJSONBOperation.YSQLJSONBOperator op = YSQLJSONBOperation.YSQLJSONBOperator.getRandom();
        YSQLDataType[] inputTypes = op.getInputDataTypes();
        
        YSQLExpression left = generateExpression(depth + 1, inputTypes[0]);
        YSQLExpression right;
        
        if (inputTypes[1] == YSQLDataType.TEXT_ARRAY) {
            // Generate text array constant
            List<String> keys = new ArrayList<>();
            int numKeys = Randomly.smallNumber() + 1;
            for (int i = 0; i < numKeys; i++) {
                keys.add("\"key" + i + "\"");
            }
            right = YSQLConstant.createTextConstant("{" + String.join(",", keys) + "}");
        } else {
            right = generateExpression(depth + 1, inputTypes[1]);
        }
        
        return new YSQLJSONBOperation(left, right, op);
    }
    
    private YSQLExpression generateJSONBFunction(int depth) {
        YSQLJSONBFunction.JSONBFunction func = YSQLJSONBFunction.JSONBFunction.getRandomNonAggregate();
        List<YSQLExpression> args = new ArrayList<>();
        
        if (func.isVariadic()) {
            // For variadic functions, generate 1-3 arguments
            int numArgs = Randomly.smallNumber() + 1;
            args.add(generateExpression(depth + 1, YSQLDataType.JSONB));
            for (int i = 1; i < numArgs; i++) {
                args.add(YSQLConstant.createTextConstant("path" + i));
            }
        } else {
            switch (func.getArity()) {
            case 1:
                if (func.getFunctionName().contains("array_elements") || func.getFunctionName().contains("array_length")) {
                    args.add(YSQLConstant.createTextConstant("[1, 2, 3, \"test\"]"));
                } else if (func.getFunctionName().contains("object_keys")) {
                    args.add(YSQLConstant.createTextConstant("{\"key1\": \"value1\", \"key2\": \"value2\"}"));
                } else {
                    args.add(generateExpression(depth + 1, YSQLDataType.JSONB));
                }
                break;
            case 2:
                if (func.getFunctionName().contains("path")) {
                    args.add(generateExpression(depth + 1, YSQLDataType.JSONB));
                    args.add(generateJSONPath());
                } else {
                    args.add(generateExpression(depth + 1, YSQLDataType.TEXT));
                    args.add(generateExpression(depth + 1, YSQLDataType.TEXT));
                }
                break;
            case 3:
                args.add(generateExpression(depth + 1, YSQLDataType.JSONB));
                args.add(YSQLConstant.createTextConstant("{key1,key2}"));
                args.add(generateExpression(depth + 1, YSQLDataType.JSONB));
                break;
            default:
                throw new AssertionError(func);
            }
        }
        
        return new YSQLJSONBFunction(func, args);
    }
    
    private YSQLExpression generateJSONBBuildFunction(int depth) {
        if (Randomly.getBoolean()) {
            // jsonb_build_array
            List<YSQLExpression> args = new ArrayList<>();
            int numArgs = Randomly.smallNumber() + 1;
            for (int i = 0; i < numArgs; i++) {
                args.add(generateExpression(depth + 1));
            }
            return new YSQLJSONBFunction(YSQLJSONBFunction.JSONBFunction.JSONB_BUILD_ARRAY, args);
        } else {
            // jsonb_build_object
            List<YSQLExpression> args = new ArrayList<>();
            int numPairs = Randomly.smallNumber() + 1;
            for (int i = 0; i < numPairs; i++) {
                args.add(YSQLConstant.createTextConstant("key" + i));
                args.add(generateExpression(depth + 1));
            }
            return new YSQLJSONBFunction(YSQLJSONBFunction.JSONBFunction.JSONB_BUILD_OBJECT, args);
        }
    }
    
    private YSQLConstant generateJSONPath() {
        return YSQLConstant.createTextConstant(Randomly.fromOptions(
            "$.key",
            "$.key1.key2",
            "$[0]",
            "$.array[*]",
            "$.key ? (@ > 5)",
            "$.**"
        ));
    }

    private YSQLExpression generateBitExpression(int depth) {
        BitExpression option;
        option = Randomly.fromOptions(BitExpression.values());
        switch (option) {
        case BINARY_OPERATION:
            YSQLBinaryBitOperation.YSQLBinaryBitOperator op = YSQLBinaryBitOperation.YSQLBinaryBitOperator.getRandom();
            
            // For XOR operations, ensure same bit string sizes
            if (op == YSQLBinaryBitOperation.YSQLBinaryBitOperator.BITWISE_XOR) {
                // Generate bit strings of the same size
                int bitSize = r.getInteger(1, 32);
                YSQLExpression left = YSQLConstant.createBitConstant(r.getInteger(0, (1 << bitSize) - 1));
                YSQLExpression right = YSQLConstant.createBitConstant(r.getInteger(0, (1 << bitSize) - 1));
                return new YSQLBinaryBitOperation(op, left, right);
            } else {
                return new YSQLBinaryBitOperation(op,
                        generateExpression(depth + 1, YSQLDataType.BIT), 
                        generateExpression(depth + 1, YSQLDataType.BIT));
            }
        default:
            throw new AssertionError();
        }
    }

    private YSQLExpression generateIntExpression(int depth) {
        IntExpression option;
        option = Randomly.fromOptions(IntExpression.values());
        switch (option) {
        case CAST:
            return new YSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(YSQLDataType.INT));
        case UNARY_OPERATION:
            YSQLExpression intExpression = generateExpression(depth + 1, YSQLDataType.INT);
            return new YSQLPrefixOperation(intExpression, Randomly.getBoolean()
                    ? YSQLPrefixOperation.PrefixOperator.UNARY_PLUS : YSQLPrefixOperation.PrefixOperator.UNARY_MINUS);
        case FUNCTION:
            return generateFunction(depth + 1, YSQLDataType.INT);
        case BINARY_ARITHMETIC_EXPRESSION:
            return new YSQLBinaryArithmeticOperation(generateExpression(depth + 1, YSQLDataType.INT),
                    generateExpression(depth + 1, YSQLDataType.INT),
                    YSQLBinaryArithmeticOperation.YSQLBinaryOperator.getRandom());
        case CASE_EXPRESSION:
            return generateCaseExpression(depth + 1, YSQLDataType.INT);
        default:
            throw new AssertionError();
        }
    }

    private YSQLExpression createColumnOfType(YSQLDataType type) {
        List<YSQLColumn> columns = filterColumns(type);
        YSQLColumn fromList = Randomly.fromList(columns);
        YSQLConstant value = rw == null ? null : rw.getValues().get(fromList);
        return YSQLColumnValue.create(fromList, value);
    }

    final List<YSQLColumn> filterColumns(YSQLDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    public YSQLExpression generateExpressionWithExpectedResult(YSQLDataType type) {
        this.expectedResult = true;
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState).setColumns(columns).setRowValue(rw);
        YSQLExpression expr;
        do {
            expr = gen.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    public List<YSQLExpression> generateExpressions(int nr) {
        List<YSQLExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(0));
        }
        return expressions;
    }

    public YSQLExpression generateExpression(YSQLDataType dataType) {
        return generateExpression(0, dataType);
    }

    public YSQLExpressionGenerator setGlobalState(YSQLGlobalState globalState) {
        this.globalState = globalState;
        return this;
    }

    public YSQLExpression generateHavingClause() {
        this.allowAggregateFunctions = true;
        YSQLExpression expression = generateExpression(YSQLDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

    public YSQLExpression generateAggregate() {
        return getAggregate(YSQLDataType.getRandomType());
    }

    private YSQLExpression getAggregate(YSQLDataType dataType) {
        List<YSQLAggregate.YSQLAggregateFunction> aggregates = YSQLAggregate.YSQLAggregateFunction
                .getAggregates(dataType);
        YSQLAggregate.YSQLAggregateFunction agg = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, agg);
    }

    public YSQLAggregate generateArgsForAggregate(YSQLDataType dataType, YSQLAggregate.YSQLAggregateFunction agg) {
        List<YSQLDataType> types = agg.getTypes(dataType);
        List<YSQLExpression> args = new ArrayList<>();
        for (YSQLDataType argType : types) {
            args.add(generateExpression(argType));
        }
        return new YSQLAggregate(args, agg);
    }

    public YSQLExpressionGenerator allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    @Override
    public YSQLExpression generatePredicate() {
        return generateExpression(YSQLDataType.BOOLEAN);
    }

    @Override
    public YSQLExpression negatePredicate(YSQLExpression predicate) {
        return new YSQLPrefixOperation(predicate, YSQLPrefixOperation.PrefixOperator.NOT);
    }

    @Override
    public YSQLExpression isNull(YSQLExpression expr) {
        return new YSQLPostfixOperation(expr, YSQLPostfixOperation.PostfixOperator.IS_NULL);
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, BETWEEN, IN_OPERATION,
        SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON, CASE_EXPRESSION
    }

    private enum RangeExpression {
        BINARY_OP
    }

    private enum TextExpression {
        CAST, FUNCTION, CONCAT, CASE_EXPRESSION
    }

    private enum BitExpression {
        BINARY_OPERATION
    }

    private enum IntExpression {
        UNARY_OPERATION, FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION, CASE_EXPRESSION
    }

}
