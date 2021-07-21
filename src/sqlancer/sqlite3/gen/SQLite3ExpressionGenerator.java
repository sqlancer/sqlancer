package sqlancer.sqlite3.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import sqlancer.sqlite3.ast.SQLite3Case.CasePair;
import sqlancer.sqlite3.ast.SQLite3Case.SQLite3CaseWithBaseExpression;
import sqlancer.sqlite3.ast.SQLite3Case.SQLite3CaseWithoutBaseExpression;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.BetweenOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.CollateOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.Join.JoinType;
import sqlancer.sqlite3.ast.SQLite3Expression.MatchOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm.Ordering;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation.BinaryOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.TypeLiteral;
import sqlancer.sqlite3.ast.SQLite3Function;
import sqlancer.sqlite3.ast.SQLite3Function.ComputableFunction;
import sqlancer.sqlite3.ast.SQLite3RowValueExpression;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import sqlancer.sqlite3.oracle.SQLite3RandomQuerySynthesizer;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3RowValue;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3ExpressionGenerator implements ExpressionGenerator<SQLite3Expression> {

    private SQLite3RowValue rw;
    private final SQLite3GlobalState globalState;
    private boolean tryToGenerateKnownResult;
    private List<SQLite3Column> columns = Collections.emptyList();
    private final Randomly r;
    private boolean deterministicOnly;
    private boolean allowMatchClause;
    private boolean allowAggregateFunctions;
    private boolean allowSubqueries;
    private boolean allowAggreates;

    public SQLite3ExpressionGenerator(SQLite3ExpressionGenerator other) {
        this.rw = other.rw;
        this.globalState = other.globalState;
        this.tryToGenerateKnownResult = other.tryToGenerateKnownResult;
        this.columns = new ArrayList<>(other.columns);
        this.r = other.r;
        this.deterministicOnly = other.deterministicOnly;
        this.allowMatchClause = other.allowMatchClause;
        this.allowAggregateFunctions = other.allowAggregateFunctions;
        this.allowSubqueries = other.allowSubqueries;
        this.allowAggreates = other.allowAggreates;
    }

    private enum LiteralValueType {
        INTEGER, NUMERIC, STRING, BLOB_LITERAL, NULL
    }

    public SQLite3ExpressionGenerator(SQLite3GlobalState globalState) {
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public SQLite3ExpressionGenerator deterministicOnly() {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(this);
        gen.deterministicOnly = true;
        return gen;
    }

    public SQLite3ExpressionGenerator allowAggregateFunctions() {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(this);
        gen.allowAggregateFunctions = true;
        return gen;
    }

    public SQLite3ExpressionGenerator setColumns(List<SQLite3Column> columns) {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(this);
        gen.columns = new ArrayList<>(columns);
        return gen;
    }

    public SQLite3ExpressionGenerator setRowValue(SQLite3RowValue rw) {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(this);
        gen.rw = rw;
        return gen;
    }

    public SQLite3ExpressionGenerator allowMatchClause() {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(this);
        gen.allowMatchClause = true;
        return gen;
    }

    public SQLite3ExpressionGenerator allowSubqueries() {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(this);
        gen.allowSubqueries = true;
        return gen;
    }

    public SQLite3ExpressionGenerator tryToGenerateKnownResult() {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(this);
        gen.tryToGenerateKnownResult = true;
        return gen;
    }

    public static SQLite3Expression getRandomLiteralValue(SQLite3GlobalState globalState) {
        return new SQLite3ExpressionGenerator(globalState).getRandomLiteralValueInternal(globalState.getRandomly());
    }

    public List<SQLite3Expression> generateOrderBys() {
        List<SQLite3Expression> expressions = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            expressions.add(generateOrderingTerm());
        }
        return expressions;
    }

    public List<Join> getRandomJoinClauses(List<SQLite3Table> tables) {
        List<Join> joinStatements = new ArrayList<>();
        if (!globalState.getDbmsSpecificOptions().testJoins) {
            return joinStatements;
        }
        if (Randomly.getBoolean() && tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            for (int i = 0; i < nrJoinClauses; i++) {
                SQLite3Expression joinClause = generateExpression();
                SQLite3Table table = Randomly.fromList(tables);
                tables.remove(table);
                JoinType options;
                options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER, JoinType.NATURAL);
                if (options == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    joinClause = null;
                }
                Join j = new SQLite3Expression.Join(table, joinClause, options);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }

    public SQLite3Expression generateOrderingTerm() {
        SQLite3Expression expr = generateExpression();
        // COLLATE is potentially already generated
        if (Randomly.getBoolean()) {
            expr = new SQLite3OrderingTerm(expr, Ordering.getRandomValue());
        }
        if (globalState.getDbmsSpecificOptions().testNullsFirstLast && Randomly.getBoolean()) {
            expr = new SQLite3PostfixText(expr, Randomly.fromOptions(" NULLS FIRST", " NULLS LAST"),
                    null /* expr.getExpectedValue() */) {
                @Override
                public boolean omitBracketsWhenPrinting() {
                    return true;
                }
            };
        }
        return expr;
    }

    /*
     * https://www.sqlite.org/syntax/literal-value.html
     */
    private SQLite3Expression getRandomLiteralValueInternal(Randomly r) {
        LiteralValueType randomLiteral = Randomly.fromOptions(LiteralValueType.values());
        switch (randomLiteral) {
        case INTEGER:
            if (Randomly.getBoolean()) {
                return SQLite3Constant.createIntConstant(r.getInteger());
            } else {
                return SQLite3Constant.createTextConstant(String.valueOf(r.getInteger()));
            }
        case NUMERIC:
            return SQLite3Constant.createRealConstant(r.getDouble());
        case STRING:
            return SQLite3Constant.createTextConstant(r.getString());
        case BLOB_LITERAL:
            return SQLite3Constant.getRandomBinaryConstant(r);
        case NULL:
            return SQLite3Constant.createNullConstant();
        default:
            throw new AssertionError(randomLiteral);
        }
    }

    enum ExpressionType {
        RANDOM_QUERY, COLUMN_NAME, LITERAL_VALUE, UNARY_OPERATOR, POSTFIX_UNARY_OPERATOR, BINARY_OPERATOR,
        BETWEEN_OPERATOR, CAST_EXPRESSION, BINARY_COMPARISON_OPERATOR, FUNCTION, IN_OPERATOR, COLLATE, CASE_OPERATOR,
        MATCH, AGGREGATE_FUNCTION, ROW_VALUE_COMPARISON, AND_OR_CHAIN
    }

    public SQLite3Expression generateExpression() {
        return getRandomExpression(0);
    }

    public List<SQLite3Expression> getRandomExpressions(int size) {
        List<SQLite3Expression> expressions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expressions.add(generateExpression());
        }
        return expressions;
    }

    public List<SQLite3Expression> getRandomExpressions(int size, int depth) {
        List<SQLite3Expression> expressions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expressions.add(getRandomExpression(depth));
        }
        return expressions;
    }

    public SQLite3Expression getRandomExpression(int depth) {
        if (allowAggreates && Randomly.getBoolean()) {
            return getAggregateFunction(depth + 1);
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth()) {
            if (Randomly.getBooleanWithRatherLowProbability() || columns.isEmpty()) {
                return getRandomLiteralValue(globalState);
            } else {
                return getRandomColumn();
            }
        }

        List<ExpressionType> list = new ArrayList<>(Arrays.asList(ExpressionType.values()));
        if (columns.isEmpty()) {
            list.remove(ExpressionType.COLUMN_NAME);
        }
        if (!allowMatchClause) {
            list.remove(ExpressionType.MATCH);
        }
        if (!allowAggregateFunctions) {
            list.remove(ExpressionType.AGGREGATE_FUNCTION);
        }
        if (!allowSubqueries) {
            list.remove(ExpressionType.RANDOM_QUERY);
        }
        if (!globalState.getDbmsSpecificOptions().testFunctions) {
            list.remove(ExpressionType.FUNCTION);
        }
        if (!globalState.getDbmsSpecificOptions().testMatch) {
            list.remove(ExpressionType.MATCH);
        }
        if (!globalState.getDbmsSpecificOptions().testIn) {
            list.remove(ExpressionType.IN_OPERATOR);
        }
        ExpressionType randomExpressionType = Randomly.fromList(list);
        switch (randomExpressionType) {
        case AND_OR_CHAIN:
            return getAndOrChain(depth + 1);
        case LITERAL_VALUE:
            return getRandomLiteralValue(globalState);
        case COLUMN_NAME:
            return getRandomColumn();
        case UNARY_OPERATOR:
            return getRandomUnaryOperator(depth + 1);
        case POSTFIX_UNARY_OPERATOR:
            return getRandomPostfixUnaryOperator(depth + 1);
        case BINARY_OPERATOR:
            return getBinaryOperator(depth + 1);
        case BINARY_COMPARISON_OPERATOR:
            return getBinaryComparisonOperator(depth + 1);
        case BETWEEN_OPERATOR:
            return getBetweenOperator(depth + 1);
        case CAST_EXPRESSION:
            return getCastOperator(depth + 1);
        case FUNCTION:
            return getFunction(globalState, depth);
        case IN_OPERATOR:
            return getInOperator(depth + 1);
        case COLLATE:
            return new CollateOperation(getRandomExpression(depth + 1), SQLite3CollateSequence.random());
        case CASE_OPERATOR:
            return getCaseOperator(depth + 1);
        case MATCH:
            return getMatchClause(depth);
        case AGGREGATE_FUNCTION:
            return getAggregateFunction(depth);
        case ROW_VALUE_COMPARISON:
            return getRowValueComparison(depth + 1);
        case RANDOM_QUERY:
            // TODO: pass schema from the outside
            // TODO: depth
            return SQLite3RandomQuerySynthesizer.generate(globalState, 1);
        default:
            throw new AssertionError(randomExpressionType);
        }
    }

    private SQLite3Expression getAndOrChain(int depth) {
        int num = Randomly.smallNumber() + 2;
        SQLite3Expression expr = getRandomExpression(depth + 1);
        for (int i = 0; i < num; i++) {
            BinaryOperator operator = Randomly.fromOptions(BinaryOperator.AND, BinaryOperator.OR);
            expr = new Sqlite3BinaryOperation(expr, getRandomExpression(depth + 1), operator);
        }
        return expr;
    }

    public SQLite3Expression getAggregateFunction(boolean asWindowFunction) {
        SQLite3AggregateFunction random = SQLite3AggregateFunction.getRandom();
        if (asWindowFunction) {
            while (/* random == SQLite3AggregateFunction.ZIPFILE || */random == SQLite3AggregateFunction.MAX
                    || random == SQLite3AggregateFunction.MIN) {
                // ZIPFILE() may not be used as a window function
                random = SQLite3AggregateFunction.getRandom();
            }
        }
        return getAggregate(0, random);
    }

    private SQLite3Expression getAggregateFunction(int depth) {
        SQLite3AggregateFunction random = SQLite3AggregateFunction.getRandom();
        return getAggregate(depth, random);
    }

    private SQLite3Expression getAggregate(int depth, SQLite3AggregateFunction random) {
        int nrArgs;
        // if (random == SQLite3AggregateFunction.ZIPFILE) {
        // nrArgs = Randomly.fromOptions(2, 4);
        // } else {
        // nrArgs = 1;
        // }
        nrArgs = 1;
        return new SQLite3Aggregate(getRandomExpressions(nrArgs, depth + 1), random);
    }

    private enum RowValueComparison {
        STANDARD_COMPARISON, BETWEEN, IN
    }

    /*
     * https://www.sqlite.org/rowvalue.html
     */
    private SQLite3Expression getRowValueComparison(int depth) {
        int size = Randomly.smallNumber() + 1;
        List<SQLite3Expression> left = getRandomExpressions(size, depth + 1);
        List<SQLite3Expression> right = getRandomExpressions(size, depth + 1);
        RowValueComparison randomOption;
        // if (Randomly.getBooleanWithSmallProbability()) {
        // // for the right hand side a random query is required, which is expensive
        // randomOption = RowValueComparison.IN;
        // } else {
        randomOption = Randomly.fromOptions(RowValueComparison.STANDARD_COMPARISON, RowValueComparison.BETWEEN);
        // }
        switch (randomOption) {
        // TODO case
        case STANDARD_COMPARISON:
            return new BinaryComparisonOperation(new SQLite3RowValueExpression(left),
                    new SQLite3RowValueExpression(right), BinaryComparisonOperator.getRandomRowValueOperator());
        case BETWEEN:
            return new BetweenOperation(getRandomRowValue(depth + 1, size), Randomly.getBoolean(),
                    new SQLite3RowValueExpression(left), new SQLite3RowValueExpression(right));
        // case IN:
        // return new SQLite3Expression.InOperation(new SQLite3RowValue(left),
        // SQLite3RandomQuerySynthesizer.generate(globalState, size));
        default:
            throw new AssertionError(randomOption);
        }
    }

    private SQLite3RowValueExpression getRandomRowValue(int depth, int size) {
        return new SQLite3RowValueExpression(getRandomExpressions(size, depth + 1));
    }

    private SQLite3Expression getMatchClause(int depth) {
        SQLite3Expression left = getRandomExpression(depth + 1);
        SQLite3Expression right;
        if (Randomly.getBoolean()) {
            right = getRandomExpression(depth + 1);
        } else {
            right = SQLite3Constant.createTextConstant(SQLite3MatchStringGenerator.generateMatchString(r));
        }
        return new MatchOperation(left, right);
    }

    private SQLite3Expression getRandomColumn() {
        SQLite3Column c = Randomly.fromList(columns);
        return new SQLite3ColumnName(c, rw == null ? null : rw.getValues().get(c));
    }

    enum Attribute {
        VARIADIC, NONDETERMINISTIC
    };

    private enum AnyFunction {
        ABS("ABS", 1), //
        CHANGES("CHANGES", 0, Attribute.NONDETERMINISTIC), //
        CHAR("CHAR", 1, Attribute.VARIADIC), //
        COALESCE("COALESCE", 2, Attribute.VARIADIC), //
        GLOB("GLOB", 2), //
        HEX("HEX", 1), //
        IFNULL("IFNULL", 2), //
        INSTR("INSTR", 2), //
        LAST_INSERT_ROWID("LAST_INSERT_ROWID", 0, Attribute.NONDETERMINISTIC), //
        LENGTH("LENGTH", 1), //
        LIKE("LIKE", 2), //
        LIKE2("LIKE", 3) {
            @Override
            List<SQLite3Expression> generateArguments(int nrArgs, int depth, SQLite3ExpressionGenerator gen) {
                List<SQLite3Expression> args = super.generateArguments(nrArgs, depth, gen);
                args.set(2, gen.getRandomSingleCharString());
                return args;
            }
        }, //
        LIKELIHOOD("LIKELIHOOD", 2), //
        LIKELY("LIKELY", 1), //
        LOAD_EXTENSION("load_extension", 1), //
        LOAD_EXTENSION2("load_extension", 2, Attribute.NONDETERMINISTIC), LOWER("LOWER", 1), //
        LTRIM1("LTRIM", 1), //
        LTRIM2("LTRIM", 2), //
        MAX("MAX", 2, Attribute.VARIADIC), //
        MIN("MIN", 2, Attribute.VARIADIC), //
        NULLIF("NULLIF", 2), //
        PRINTF("PRINTF", 1, Attribute.VARIADIC), //
        QUOTE("QUOTE", 1), //
        ROUND("ROUND", 2), //
        RTRIM("RTRIM", 1), //
        SOUNDEX("soundex", 1), //
        SQLITE_COMPILEOPTION_GET("SQLITE_COMPILEOPTION_GET", 1, Attribute.NONDETERMINISTIC), //
        SQLITE_COMPILEOPTION_USED("SQLITE_COMPILEOPTION_USED", 1, Attribute.NONDETERMINISTIC), //
        // SQLITE_OFFSET(1), //
        SQLITE_SOURCE_ID("SQLITE_SOURCE_ID", 0, Attribute.NONDETERMINISTIC),
        SQLITE_VERSION("SQLITE_VERSION", 0, Attribute.NONDETERMINISTIC), //
        SUBSTR("SUBSTR", 2), //
        TOTAL_CHANGES("TOTAL_CHANGES", 0, Attribute.NONDETERMINISTIC), //
        TRIM("TRIM", 1), //
        TYPEOF("TYPEOF", 1), //
        UNICODE("UNICODE", 1), UNLIKELY("UNLIKELY", 1), //
        UPPER("UPPER", 1), // "ZEROBLOB"
        // ZEROBLOB("ZEROBLOB", 1),
        DATE("DATE", 3, Attribute.VARIADIC), //
        TIME("TIME", 3, Attribute.VARIADIC), //
        DATETIME("DATETIME", 3, Attribute.VARIADIC), //
        JULIANDAY("JULIANDAY", 3, Attribute.VARIADIC), //
        STRFTIME("STRFTIME", 3, Attribute.VARIADIC),
        // json functions
        JSON("json", 1), //
        JSON_ARRAY("json_array", 2, Attribute.VARIADIC), JSON_ARRAY_LENGTH("json_array_length", 1), //
        JSON_ARRAY_LENGTH2("json_array_length", 2), //
        JSON_EXTRACT("json_extract", 2, Attribute.VARIADIC), JSON_INSERT("json_insert", 3, Attribute.VARIADIC),
        JSON_OBJECT("json_object", 2, Attribute.VARIADIC), JSON_PATCH("json_patch", 2),
        JSON_REMOVE("json_remove", 2, Attribute.VARIADIC), JSON_TYPE("json_type", 1), //
        JSON_VALID("json_valid", 1), //
        JSON_QUOTE("json_quote", 1), //

        RTREENODE("rtreenode", 2),

        // FTS
        HIGHLIGHT("highlight", 4);

        // testing functions
        // EXPR_COMPARE("expr_compare", 2), EXPR_IMPLIES_EXPR("expr_implies_expr", 2);

        // fts5_decode("fts5_decode", 2),
        // fts5_decode_none("fts5_decode_none", 2),
        // fts5_expr("fts5_expr", 1),
        // fts5_expr_tcl("fts5_expr_tcl", 1),
        // fts5_fold("fts5_fold", 1),
        // fts5_isalnum("fts5_isalnum", 1);

        private int minNrArgs;
        private boolean variadic;
        private boolean deterministic;
        private String name;

        AnyFunction(String name, int minNrArgs, Attribute... attributes) {
            this.name = name;
            List<Attribute> attrs = Arrays.asList(attributes);
            this.minNrArgs = minNrArgs;
            this.variadic = attrs.contains(Attribute.VARIADIC);
            this.deterministic = !attrs.contains(Attribute.NONDETERMINISTIC);
        }

        public boolean isVariadic() {
            return variadic;
        }

        public int getMinNrArgs() {
            return minNrArgs;
        }

        static AnyFunction getRandom(SQLite3GlobalState globalState) {
            return Randomly.fromList(getAllFunctions(globalState));
        }

        private static List<AnyFunction> getAllFunctions(SQLite3GlobalState globalState) {
            List<AnyFunction> functions = new ArrayList<>(Arrays.asList(AnyFunction.values()));
            if (!globalState.getDbmsSpecificOptions().testSoundex) {
                boolean removed = functions.removeIf(f -> f.name.equals("soundex"));
                if (!removed) {
                    throw new IllegalStateException();
                }
            }
            return functions;
        }

        static AnyFunction getRandomDeterministic(SQLite3GlobalState globalState) {
            return Randomly.fromList(
                    getAllFunctions(globalState).stream().filter(f -> f.deterministic).collect(Collectors.toList()));
        }

        @Override
        public String toString() {
            return name;
        }

        List<SQLite3Expression> generateArguments(int nrArgs, int depth, SQLite3ExpressionGenerator gen) {
            List<SQLite3Expression> expressions = new ArrayList<>();
            for (int i = 0; i < nrArgs; i++) {
                expressions.add(gen.getRandomExpression(depth + 1));
            }
            return expressions;
        }
    }

    private SQLite3Expression getFunction(SQLite3GlobalState globalState, int depth) {
        if (tryToGenerateKnownResult || Randomly.getBoolean()) {
            return getComputableFunction(depth + 1);
        } else {
            AnyFunction randomFunction;
            if (deterministicOnly) {
                randomFunction = AnyFunction.getRandomDeterministic(globalState);
            } else {
                randomFunction = AnyFunction.getRandom(globalState);
            }
            int nrArgs = randomFunction.getMinNrArgs();
            if (randomFunction.isVariadic()) {
                nrArgs += Randomly.smallNumber();
            }
            List<SQLite3Expression> expressions = randomFunction.generateArguments(nrArgs, depth + 1, this);
            return new SQLite3Expression.Function(randomFunction.toString(),
                    expressions.toArray(new SQLite3Expression[0]));
        }

    }

    protected SQLite3Expression getRandomSingleCharString() {
        String s;
        do {
            s = r.getString();
        } while (s.isEmpty());
        return new SQLite3TextConstant(String.valueOf(s.charAt(0)));
    }

    private SQLite3Expression getCaseOperator(int depth) {
        int nrCaseExpressions = 1 + Randomly.smallNumber();
        CasePair[] pairs = new CasePair[nrCaseExpressions];
        for (int i = 0; i < pairs.length; i++) {
            SQLite3Expression whenExpr = getRandomExpression(depth + 1);
            SQLite3Expression thenExpr = getRandomExpression(depth + 1);
            CasePair pair = new CasePair(whenExpr, thenExpr);
            pairs[i] = pair;
        }
        SQLite3Expression elseExpr;
        if (Randomly.getBoolean()) {
            elseExpr = getRandomExpression(depth + 1);
        } else {
            elseExpr = null;
        }
        if (Randomly.getBoolean()) {
            return new SQLite3CaseWithoutBaseExpression(pairs, elseExpr);
        } else {
            SQLite3Expression baseExpr = getRandomExpression(depth + 1);
            return new SQLite3CaseWithBaseExpression(baseExpr, pairs, elseExpr);
        }
    }

    private SQLite3Expression getCastOperator(int depth) {
        SQLite3Expression expr = getRandomExpression(depth + 1);
        TypeLiteral type = new SQLite3Expression.TypeLiteral(
                Randomly.fromOptions(SQLite3Expression.TypeLiteral.Type.values()));
        return new SQLite3Expression.Cast(type, expr);
    }

    private SQLite3Expression getComputableFunction(int depth) {
        ComputableFunction func = ComputableFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        SQLite3Expression[] args = new SQLite3Expression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = getRandomExpression(depth + 1);
            if (i == 0 && Randomly.getBoolean()) {
                args[i] = new SQLite3Distinct(args[i]);
            }
        }
        return new SQLite3Function(func, args);
    }

    private SQLite3Expression getBetweenOperator(int depth) {
        boolean tr = Randomly.getBoolean();
        SQLite3Expression expr = getRandomExpression(depth + 1);
        SQLite3Expression left = getRandomExpression(depth + 1);
        SQLite3Expression right = getRandomExpression(depth + 1);
        return new SQLite3Expression.BetweenOperation(expr, tr, left, right);
    }

    // TODO: incomplete
    private SQLite3Expression getBinaryOperator(int depth) {
        SQLite3Expression leftExpression = getRandomExpression(depth + 1);
        // TODO: operators
        BinaryOperator operator = BinaryOperator.getRandomOperator();
        // while (operator == BinaryOperator.DIVIDE) {
        // operator = BinaryOperator.getRandomOperator();
        // }
        SQLite3Expression rightExpression = getRandomExpression(depth + 1);
        return new SQLite3Expression.Sqlite3BinaryOperation(leftExpression, rightExpression, operator);
    }

    private SQLite3Expression getInOperator(int depth) {
        SQLite3Expression leftExpression = getRandomExpression(depth + 1);
        List<SQLite3Expression> right = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            right.add(getRandomExpression(depth + 1));
        }
        return new SQLite3Expression.InOperation(leftExpression, right);
    }

    private SQLite3Expression getBinaryComparisonOperator(int depth) {
        SQLite3Expression leftExpression = getRandomExpression(depth + 1);
        BinaryComparisonOperator operator = BinaryComparisonOperator.getRandomOperator();
        SQLite3Expression rightExpression = getRandomExpression(depth + 1);
        return new SQLite3Expression.BinaryComparisonOperation(leftExpression, rightExpression, operator);
    }

    // complete
    private SQLite3Expression getRandomPostfixUnaryOperator(int depth) {
        SQLite3Expression subExpression = getRandomExpression(depth + 1);
        PostfixUnaryOperator operator = PostfixUnaryOperator.getRandomOperator();
        return new SQLite3Expression.SQLite3PostfixUnaryOperation(operator, subExpression);
    }

    // complete
    public SQLite3Expression getRandomUnaryOperator(int depth) {
        SQLite3Expression subExpression = getRandomExpression(depth + 1);
        UnaryOperator unaryOperation = Randomly.fromOptions(UnaryOperator.values());
        return new SQLite3UnaryOperation(unaryOperation, subExpression);
    }

    public SQLite3Expression getHavingClause() {
        allowAggreates = true;
        return generateExpression();
    }

    @Override
    public SQLite3Expression generatePredicate() {
        return generateExpression();
    }

    @Override
    public SQLite3Expression negatePredicate(SQLite3Expression predicate) {
        return new SQLite3UnaryOperation(UnaryOperator.NOT, predicate);
    }

    @Override
    public SQLite3Expression isNull(SQLite3Expression expr) {
        return new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.ISNULL, expr);
    }

    public SQLite3Expression generateResultKnownExpression() {
        SQLite3Expression expr;
        do {
            expr = generateExpression();
        } while (expr.getExpectedValue() == null);
        return expr;
    }

}
