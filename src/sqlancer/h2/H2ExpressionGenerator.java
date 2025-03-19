package sqlancer.h2;

import java.util.List;
import java.util.stream.Collectors;

import java.util.Random;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2CompositeDataType;
import sqlancer.h2.H2Schema.H2DataType;
import sqlancer.h2.H2Schema.H2Table;
import sqlancer.h2.ast.H2BetweenOperation;
import sqlancer.h2.ast.H2BinaryOperation;
import sqlancer.h2.ast.H2CaseOperation;
import sqlancer.h2.ast.H2CastNode;
import sqlancer.h2.ast.H2ColumnReference;
import sqlancer.h2.ast.H2Constant;
import sqlancer.h2.ast.H2Expression;
import sqlancer.h2.ast.H2InOperation;
import sqlancer.h2.ast.H2Join;
import sqlancer.h2.ast.H2Select;
import sqlancer.h2.ast.H2TableReference;
import sqlancer.h2.ast.H2UnaryPostfixOperation;
import sqlancer.h2.ast.H2UnaryPrefixOperation;

public class H2ExpressionGenerator extends UntypedExpressionGenerator<H2Expression, H2Column>
        implements TLPWhereGenerator<H2Select, H2Join, H2Expression, H2Table, H2Column> {

    private final H2GlobalState globalState;
    private List<H2Table> tables;

    public H2ExpressionGenerator(H2GlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        BINARY_COMPARISON, BINARY_LOGICAL, UNARY_POSTFIX, UNARY_PREFIX, IN, BETWEEN, CASE, BINARY_ARITHMETIC, CAST,
        FUNCTION;
    }

    @Override
    protected H2Expression generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        Expression expr = Randomly.fromOptions(Expression.values());
        switch (expr) {
        case BINARY_COMPARISON:
            Operator op = H2BinaryComparisonOperator.getRandom();
            return new H2BinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = H2BinaryLogicalOperator.getRandom();
            return new H2BinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case UNARY_POSTFIX:
            op = H2UnaryPostfixOperator.getRandom();
            return new H2UnaryPostfixOperation(generateExpression(depth + 1), op);
        case UNARY_PREFIX:
            return new H2UnaryPrefixOperation(generateExpression(depth + 1), H2UnaryPrefixOperator.getRandom());
        case IN:
            return new H2InOperation(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case BETWEEN:
            return new H2BetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new H2CaseOperation(generateExpression(depth + 1), generateExpressions(nr, depth + 1),
                    generateExpressions(nr, depth + 1), generateExpression(depth + 1));
        case BINARY_ARITHMETIC:
            return new H2BinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    H2BinaryArithmeticOperator.getRandom());
        case CAST:
            return new H2CastNode(generateExpression(depth + 1), H2CompositeDataType.getRandom());
        case FUNCTION:
            H2Function func = H2Function.getRandom();
            return new sqlancer.h2.ast.H2Function<>(generateExpressions(func.getNrArgs()), func);
        default:
            throw new AssertionError();
        }
    }

public enum H2Function {

    // numeric functions
    ABS(1), //
    ACOS(1), //
    ASIN(1), //
    ATAN(1), //
    COS(1), //
    COSH(1), //
    COT(1), //
    SIN(1), //
    SINH(1), //
    TAN(1), //
    TANH(1), //
    ATAN2(2), //
    BITAND(2), //
    BITGET(2), //
    BITNOT(1), //
    BITOR(2), //
    BITXOR(2), //
    LSHIFT(2), //
    RSHIFT(2), //
    MOD(2), //
    CEILING(1), //
    DEGREES(1), //
    EXP(1), //
    FLOOR(1), //
    LN(1), //
    LOG(2), //
    LOG10(1), //
    ORA_HASH(1), //
    RADIANS(1), //
    SQRT(1), //
    PI(0), //
    POWER(2), //
    ROUND(2), //
    ROUNDMAGIC(1), //
    SIGN(1), //
    TRUNCATE(2), //
    COMPRESS(1), //
    ZERO(0), //
    // string functions
    ASCII(1), //
    BIT_LENGTH(1), //
    LENGTH(1), //
    OCTET_LENGTH(1), //
    CHAR(1), //
    CONCAT(2, true), //
    CONCAT_WS(3, true), //
    DIFFERENCE(2), //
    HEXTORAW(1), //
    RAWTOHEX(1), //
    INSTR(3), //
    INSERT(4), //
    LOWER(1), //
    UPPER(1), //
    LEFT(2), //
    RIGHT(2), //
    LOCATE(3), //
    POSITION(2), //
    LTRIM(1), //
    RTRIM(1), //
    TRIM(1), //
    REGEXP_REPLACE(3), //
    REGEXP_LIKE(2), //
    REPLACE(3), //
    SOUNDEX(1), //
    STRINGDECODE(1), //
    STRINGENCODE(1), //
    STRINGTOUTF8(1), //
    SUBSTRING(2), //
    UTF8TOSTRING(1), //
    QUOTE_IDENT(1), //
    XMLATTR(2), //
    XMLNODE(1), //
    XMLCOMMENT(1), //
    XMLCDATA(1), //
    XMLSTARTDOC(0), //
    XMLTEXT(1), //
    TRANSLATE(3), //
    // time and date functions
    CURRENT_DATE(0), //
    CURRENT_TIME(0), //
    CURRENT_TIMESTAMP(0), //
    DATEADD(3), //
    DATEDIFF(3), //
    EXTRACT(2), //
    NOW(0), //
    // array functions
    ARRAY_LENGTH(1), //
    ARRAY_GET(2), //
    ARRAY_CONTAINS(2), //
    ARRAY_APPEND(2), //
    ARRAY_REMOVE(2), //
    ARRAY_SLICE(3), //
    ARRAY_POSITION(2), //
    ARRAY_SORT(1), //
    // JSON functions
    JSON_ARRAY(1, true), // Variadic function
    JSON_OBJECT(1, true), // Variadic function
    JSON_VALUE(2), //
    JSON_QUERY(2), //
    JSON_EXISTS(2), //
    // system functions
    CASEWHEN(3), //
    COALESCE(1, true), //
    CURRENT_SCHEMA(0), //
    CURRENT_CATALOG(0), //
    DATABASE_PATH(0), //
    DECODE(3, true), //
    GREATEST(2, true), //
    IFNULL(2), //
    LEAST(2, true), //
    LOCK_MODE(0), //
    LOCK_TIMEOUT(0), //
    NULLIF(2), //
    NVL2(3), //
    READONLY(0), //
    SESSION_ID(0), //
    TRUNCATE_VALUE(3), //
    USER(0);

    private int nrArgs;
    private boolean isVariadic;

    H2Function(int nrArgs) {
        this(nrArgs, false);
    }

    H2Function(int nrArgs, boolean isVariadic) {
        this.nrArgs = nrArgs;
        this.isVariadic = isVariadic;
    }

    public static H2Function getRandom() {
        H2Function[] values = values();
        return values[new Random().nextInt(values.length)];
    }

    public int getNrArgs() {
        if (isVariadic) {
            return new Random().nextInt(5) + nrArgs;
        } else {
            return nrArgs;
        }
    }
}

    @Override
    protected H2Expression generateColumn() {
        return new H2ColumnReference(Randomly.fromList(columns));
    }

    @Override
    public H2Expression generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return H2Constant.createNullConstant();
        }
        switch (H2DataType.getRandom()) {
        case INT:
            return H2Constant.createIntConstant(globalState.getRandomly().getInteger());
        case BOOL:
            return H2Constant.createBoolConstant(Randomly.getBoolean());
        case VARCHAR:
            return H2Constant.createStringConstant(globalState.getRandomly().getString());
        case DOUBLE:
            return H2Constant.createDoubleConstant(globalState.getRandomly().getDouble());
        case BINARY:
            return H2Constant.createBinaryConstant(globalState.getRandomly().getInteger());
        default:
            throw new AssertionError();
        }
    }

    public enum H2UnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL"), IS_TRUE("IS TRUE"), IS_NOT_TRUE("IS NOT TRUE"),
        IS_FALSE("IS FALSE"), IS_NOT_FALSE("IS NOT FALSE"), IS_UNKNOWN("IS NOT UNKNOWN");

        private String textRepr;

        H2UnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static H2UnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum H2UnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        H2UnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static H2UnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum H2BinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%");

        private String textRepr;

        H2BinaryArithmeticOperator(String textRepr) {
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

    public enum H2BinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum H2BinaryComparisonOperator implements Operator {

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        IS_DISTINCT_FROM("IS DISTINCT FROM"), IS_NOT_DISTINCT("IS NOT DISTINCT FROM"), LIKE("LIKE"),
        NOT_LIKE("NOT LIKE"), REGEXP("REGEXP"), NOT_REGEXP("NOT REGEXP");

        private String textRepr;

        H2BinaryComparisonOperator(String textRepr) {
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

    @Override
    public H2Expression negatePredicate(H2Expression predicate) {
        return new H2UnaryPrefixOperation(predicate, H2UnaryPrefixOperator.NOT);
    }

    @Override
    public H2Expression isNull(H2Expression expr) {
        return new H2UnaryPostfixOperation(expr, H2UnaryPostfixOperator.IS_NULL);
    }

    @Override
    public TLPWhereGenerator<H2Select, H2Join, H2Expression, H2Table, H2Column> setTablesAndColumns(
            AbstractTables<H2Table, H2Column> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public H2Expression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public H2Select generateSelect() {
        return new H2Select();
    }

    @Override
    public List<H2Join> getRandomJoinClauses() {
        List<H2TableReference> tableList = tables.stream().map(t -> new H2TableReference(t))
                .collect(Collectors.toList());
        List<H2Join> joins = H2Join.getJoins(tableList, globalState);
        tables = tableList.stream().map(t -> t.getTable()).collect(Collectors.toList());
        return joins;
    }

    @Override
    public List<H2Expression> getTableRefs() {
        return tables.stream().map(t -> new H2TableReference(t)).collect(Collectors.toList());
    }

    @Override
    public List<H2Expression> generateFetchColumns(boolean shouldCreateDummy) {
        if (shouldCreateDummy && Randomly.getBoolean()) {
            return List.of(new H2ColumnReference(new H2Column("*", null)));
        }
        return Randomly.nonEmptySubset(this.columns).stream().map(c -> new H2ColumnReference(c))
                .collect(Collectors.toList());
    }
}
