package sqlancer.voltdb.gen;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBCompositeDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;
import sqlancer.voltdb.ast.VoltDBAggregate;
import sqlancer.voltdb.ast.VoltDBAggregate.VoltDBAggregateFunction;
import sqlancer.voltdb.ast.VoltDBAlias;
import sqlancer.voltdb.ast.VoltDBBetweenOperation;
import sqlancer.voltdb.ast.VoltDBBinaryOperation;
import sqlancer.voltdb.ast.VoltDBBinaryOperation.VoltDBBinaryOperator;
import sqlancer.voltdb.ast.VoltDBCase;
import sqlancer.voltdb.ast.VoltDBColumnReference;
import sqlancer.voltdb.ast.VoltDBConstant;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBFunction;
import sqlancer.voltdb.ast.VoltDBFunction.VoltDBFunctionName;
import sqlancer.voltdb.ast.VoltDBInOperation;
import sqlancer.voltdb.ast.VoltDBJoin;
import sqlancer.voltdb.ast.VoltDBKeyword;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.ast.VoltDBSubquery;
import sqlancer.voltdb.ast.VoltDBTableReference;
import sqlancer.voltdb.ast.VoltDBUnaryPostfixOperation;
import sqlancer.voltdb.ast.VoltDBUnaryPostfixOperation.VoltDBUnaryPostfixOperator;
import sqlancer.voltdb.ast.VoltDBUnaryPrefixOperation;
import sqlancer.voltdb.ast.VoltDBUnaryPrefixOperation.VoltDBUnaryPrefixOperator;

/**
 * Typed expressions, and the queries the NoREC oracle compares.
 *
 * <p>
 * Every operand is generated for one {@link Kind}, so that VoltDB never rejects an expression for mixing types
 * ("incompatible data types in combination"). NULL literals carry a type (CAST(NULL AS ...)), as VoltDB requires.
 *
 * <p>
 * VoltDB plans a query over several partitioned tables only when they are joined on their partitioning columns. The
 * FROM clause therefore holds at most one partitioned table without a join condition; every other partitioned table is
 * joined to it on the partitioning columns, and one whose partitioning column cannot be compared with it is left out.
 * The join is in the FROM clause, so both NoREC queries have it.
 */
public final class VoltDBExpressionGenerator
        extends TypedExpressionGenerator<VoltDBExpression, VoltDBColumn, VoltDBExpressionGenerator.Kind>
        implements NoRECGenerator<VoltDBSelect, VoltDBJoin, VoltDBExpression, VoltDBTable, VoltDBColumn> {

    // The type classes VoltDB compares and combines freely within, and not across.
    public enum Kind {
        BOOLEAN, NUMERIC, STRING, VARBINARY, TEMPORAL;

        public static Kind fromType(VoltDBDataType type) {
            switch (type) {
            case BOOLEAN:
                return BOOLEAN;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DECIMAL:
                return NUMERIC;
            case VARCHAR:
                return STRING;
            case VARBINARY:
                return VARBINARY;
            case TIMESTAMP:
            case DATE:
                return TEMPORAL;
            default:
                throw new AssertionError(type);
            }
        }
    }

    private enum BooleanExpression {
        COMPARISON, AND_OR, NOT, IS_NULL, BETWEEN, IN, LIKE
    }

    private enum NumericDomain {
        ANY, EXACT, FLOAT;

        boolean contains(VoltDBDataType type) {
            return this == ANY || (type == VoltDBDataType.FLOAT) == (this == FLOAT);
        }
    }

    private enum NumericExpression {
        UNARY_MINUS, ARITHMETIC, ABS, FLOOR_CEILING, MOD, CHAR_LENGTH, CASE, COALESCE,
        // milestone 19
        FLOAT_MATH, DATE_PART, SINCE_EPOCH, JSON_ARRAY_LENGTH
    }

    private enum StringExpression {
        CONCAT, SUBSTRING, CASE_CONVERSION, TRIM, CASE, COALESCE,
        // milestone 19
        JSON
    }

    private enum TemporalExpression {
        DATEADD, TRUNCATE, TO_TIMESTAMP
    }

    // JSON documents for the JSON functions (milestone 19); FIELD's names and ARRAY_ELEMENT's indexes reach into them
    private static final String[] JSON_DOCUMENTS = { "{\"a\":1,\"b\":[1,2,3],\"c\":\"x\"}",
            "{\"a\":\"one\",\"b\":[],\"d\":{\"e\":null}}", "{\"b\":[\"p\",\"q\"],\"c\":true}", "[1,2]", "{}",
            "{\"a\":-2.5,\"c\":\"\"}" };

    private final VoltDBGlobalState state;
    private final boolean avoidKnownBugs;

    // the FROM list, comma-separated; the joins attach to its last table
    private List<VoltDBTable> fromTables = new ArrayList<>();
    private List<VoltDBJoin> joins = new ArrayList<>();
    // the partitioned table every other partitioned table in FROM is joined to on their partitioning columns, or null
    // when FROM reads replicated tables only
    private VoltDBTable partitionAnchor;
    private boolean hasOuterJoin;
    // whether the FROM clause reads only replicated tables, which VoltDB can combine with UNION ALL
    private boolean replicatedOnly;
    // ENG-30513: which numeric types the current comparison draws its operands from (see inDomain)
    private NumericDomain numericDomain = NumericDomain.ANY;
    // ENG-30545: the one temporal type the current comparison draws its operands from, or null for both
    private VoltDBDataType temporalType;
    // how many comparison or IN operands enclose the expression being generated
    private int comparandNesting;
    // while a HAVING clause is generated: the columns aggregates read (columns then holds the grouping columns), and
    // whether an aggregate was generated
    private List<VoltDBColumn> aggregateColumns;
    private boolean aggregated;
    // ENG-30531 (see setNoFullJoins)
    private boolean noFullJoins;
    // ENG-30533 (see setUngroupedAggregates)
    private boolean ungroupedAggregates;
    // whether an aggregate's argument is being generated (ENG-30534)
    private boolean inAggregate;
    // how many functions that type their result from their arguments (ABS, FLOOR, CEILING, MOD, CASE, COALESCE)
    // enclose the expression being generated (ENG-30550)
    private int typedFunctionNesting;
    // whether expressions may hold subqueries (see setSubqueries); the replicated tables they may read; how many
    // subqueries enclose the expression being generated; and the number of the next subquery's alias
    private boolean subqueries;
    private List<VoltDBTable> subqueryTables = List.of();
    private int subqueryNesting;
    private int subqueryAliases;
    // while greater than 0, no subquery (see withoutSubqueries)
    private int subqueriesBlocked;

    private static final int MAX_SUBQUERY_NESTING = 2;

    public VoltDBExpressionGenerator(VoltDBGlobalState state) {
        this.state = state;
        this.avoidKnownBugs = state.getDbmsSpecificOptions().avoidKnownBugs;
    }

    @Override
    protected VoltDBExpression generateExpression(Kind kind, int depth) {
        if (depth >= state.getOptions().getMaxExpressionDepth() || Randomly.getBooleanWithSmallProbability()) {
            return generateLeafNode(kind);
        }
        if (kind != Kind.BOOLEAN && canGenerateScalarSubquery(kind) && Randomly.getBooleanWithSmallProbability()) {
            return generateScalarSubquery(kind, depth);
        }
        switch (kind) {
        case BOOLEAN:
            return generateBooleanExpression(depth);
        case NUMERIC:
            return generateNumericExpression(depth);
        case STRING:
            if (Randomly.getBoolean()) {
                return generateStringExpression(depth);
            }
            return generateLeafNode(kind);
        case TEMPORAL:
            if (Randomly.getBoolean()) {
                return generateTemporalExpression(depth);
            }
            return generateLeafNode(kind);
        default:
            return generateLeafNode(kind);
        }
    }

    // DATEADD(unit, n, t) and TRUNCATE(unit, t), of t's type, and TO_TIMESTAMP(unit, n), a TIMESTAMP (milestone 19)
    private VoltDBExpression generateTemporalExpression(int depth) {
        TemporalExpression choice = Randomly.fromOptions(TemporalExpression.values());
        if (choice == TemporalExpression.TO_TIMESTAMP && temporalType == VoltDBDataType.DATE) {
            choice = TemporalExpression.DATEADD;
        }
        Randomly r = state.getRandomly();
        switch (choice) {
        case DATEADD:
            // ENG-30611: DATEADD(YEAR, n, d) on a DATE February 29 into a non-leap year is an error, and with a DATE
            // literal it aborts the server; unless --avoid-known-bugs is false, there is no YEAR (MONTH clamps)
            String unit;
            do {
                unit = Randomly.fromOptions("YEAR", "QUARTER", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
                        "MILLISECOND", "MICROSECOND");
            } while (avoidKnownBugs && unit.equals("YEAR"));
            return new VoltDBFunction(List.of(new VoltDBKeyword(unit),
                    Randomly.getBoolean() ? integerConstant(r.getInteger(-400, 400)) : generateIntegerOperand(),
                    generateExpression(Kind.TEMPORAL, depth + 1)), VoltDBFunctionName.DATEADD);
        case TRUNCATE:
            return new VoltDBFunction(
                    List.of(new VoltDBKeyword(Randomly.fromOptions("YEAR", "QUARTER", "MONTH", "DAY", "HOUR", "MINUTE",
                            "SECOND", "MILLISECOND")), generateExpression(Kind.TEMPORAL, depth + 1)),
                    VoltDBFunctionName.TRUNCATE);
        case TO_TIMESTAMP:
            return new VoltDBFunction(
                    List.of(new VoltDBKeyword(Randomly.fromOptions("SECOND", "MILLISECOND", "MICROSECOND")),
                            Randomly.getBoolean() ? integerConstant(r.getLong(-100_000_000_000L, 100_000_000_000L))
                                    : generateIntegerOperand()),
                    VoltDBFunctionName.TO_TIMESTAMP);
        default:
            throw new AssertionError(choice);
        }
    }

    // A TIMESTAMP expression, for SINCE_EPOCH, which rejects a DATE ("Type DATE can't be cast as TIMESTAMP")
    private VoltDBExpression generateTimestamp(int depth) {
        VoltDBDataType outer = temporalType;
        temporalType = VoltDBDataType.TIMESTAMP;
        try {
            return canGenerateColumnOfType(Kind.TEMPORAL) || Randomly.getBoolean()
                    ? generateExpression(Kind.TEMPORAL, depth + 1) : generateConstant(Kind.TEMPORAL, false);
        } finally {
            temporalType = outer;
        }
    }

    // A JSON document: one of JSON_DOCUMENTS, or a string expression, which is mostly not JSON ("Invalid JSON")
    private VoltDBExpression generateJsonDocument(int depth) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return generateExpression(Kind.STRING, depth + 1);
        }
        return new VoltDBConstant(VoltDBConstantGenerator.quote(Randomly.fromOptions(JSON_DOCUMENTS)));
    }

    // FIELD(doc, 'b'), the array most documents hold
    private VoltDBExpression generateJsonArray(int depth) {
        return new VoltDBFunction(List.of(generateJsonDocument(depth), new VoltDBConstant("'b'")),
                VoltDBFunctionName.FIELD);
    }

    private VoltDBExpression generateStringExpression(int depth) {
        switch (Randomly.fromOptions(StringExpression.values())) {
        case CONCAT:
            // ENG-30546: a concatenation whose result is empty, such as v || v with v = '', is NULL at run time,
            // so unless --avoid-known-bugs is false its right operand is a constant that is not empty
            VoltDBExpression right = avoidKnownBugs ? generateNonEmptyStringConstant()
                    : generateExpression(Kind.STRING, depth + 1);
            return new VoltDBBinaryOperation(generateExpression(Kind.STRING, depth + 1), right,
                    VoltDBBinaryOperator.CONCAT);
        case SUBSTRING:
            // SUBSTRING(s, start [, length]) counts characters from 1; a start below 1 shortens the length, and a
            // negative length is an error, so the length is a constant of 0 or more
            Randomly r = state.getRandomly();
            List<VoltDBExpression> args = new ArrayList<>();
            // ENG-30591: an IN predicate anywhere in SUBSTRING's string argument, as in a CASE condition, crashes the
            // parser, so unless --avoid-known-bugs is false the argument has none, as in a comparison's operands
            // ENG-16927 (comment): a subquery anywhere in SUBSTRING's or TRIM's string argument crashes the parser
            // (NullPointerException in QueryExpression.getColumns), so unless --avoid-known-bugs is false it has none
            int nesting = comparandNesting;
            comparandNesting += avoidKnownBugs ? 1 : 0;
            try {
                args.add(avoidKnownBugs ? withoutSubqueries(() -> generateExpression(Kind.STRING, depth + 1))
                        : generateExpression(Kind.STRING, depth + 1));
            } finally {
                comparandNesting = nesting;
            }
            args.add(integerConstant(r.getInteger(-2, 7)));
            if (Randomly.getBoolean()) {
                args.add(integerConstant(r.getInteger(0, 5)));
            }
            return new VoltDBFunction(args, VoltDBFunctionName.SUBSTRING);
        case CASE_CONVERSION:
            return new VoltDBFunction(List.of(generateExpression(Kind.STRING, depth + 1)),
                    Randomly.fromOptions(VoltDBFunctionName.UPPER, VoltDBFunctionName.LOWER));
        case TRIM:
            VoltDBFunctionName trim = Randomly.fromOptions(VoltDBFunctionName.TRIM, VoltDBFunctionName.LTRIM,
                    VoltDBFunctionName.RTRIM);
            // ENG-16927 (see SUBSTRING): TRIM(x) is written LTRIM(RTRIM(x)) while avoiding known bugs, so only a
            // TRIM generated with --avoid-known-bugs false can hold a subquery
            VoltDBExpression trimmed = generateExpression(Kind.STRING, depth + 1);
            if (trim == VoltDBFunctionName.TRIM && avoidKnownBugs) {
                // ENG-30592: a parameter inside TRIM(x) is counted twice ("Incorrect number of parameters passed"),
                // so unless --avoid-known-bugs is false, TRIM(x) is written LTRIM(RTRIM(x))
                return new VoltDBFunction(List.of(new VoltDBFunction(List.of(trimmed), VoltDBFunctionName.RTRIM)),
                        VoltDBFunctionName.LTRIM);
            }
            return new VoltDBFunction(List.of(trimmed), trim);
        case CASE:
            return generateCase(Kind.STRING, depth);
        case COALESCE:
            return generateCoalesce(Kind.STRING, depth);
        case JSON:
            switch (Randomly.fromOptions(0, 1, 2)) {
            case 0:
                return new VoltDBFunction(
                        List.of(generateJsonDocument(depth),
                                new VoltDBConstant(Randomly.fromOptions("'a'", "'b'", "'c'", "'d'", "'zz'"))),
                        VoltDBFunctionName.FIELD);
            case 1:
                return new VoltDBFunction(
                        List.of(generateJsonArray(depth), integerConstant(state.getRandomly().getInteger(-1, 4))),
                        VoltDBFunctionName.ARRAY_ELEMENT);
            default:
                return new VoltDBFunction(
                        List.of(generateJsonDocument(depth), new VoltDBConstant(Randomly.fromOptions("'a'", "'z'")),
                                new VoltDBConstant(Randomly.fromOptions("'5'", "'\"s\"'", "'null'", "'[1]'"))),
                        VoltDBFunctionName.SET_FIELD);
            }
        default:
            throw new AssertionError();
        }
    }

    private static VoltDBConstant integerConstant(long value) {
        return new VoltDBConstant(value < 0 ? "(" + value + ")" : Long.toString(value));
    }

    // CASE WHEN c1 THEN e1 [WHEN c2 THEN e2] [ELSE e] END, its results of the given kind
    private VoltDBExpression generateCase(Kind kind, int depth) {
        if (hasOuterJoin && avoidKnownBugs) {
            // ENG-30497 (comment): a CASE over an outer join's inner table in WHERE turns the join into an inner join,
            // though the CASE can be non-NULL for the null-extended row; COALESCE is handled correctly
            return generateCoalesce(kind, depth);
        }
        List<VoltDBExpression> conditions = new ArrayList<>();
        List<VoltDBExpression> results = new ArrayList<>();
        typedFunctionNesting++;
        try {
            // ENG-30590: a DECIMAL literal as the result of a CASE with two or more WHENs fails to compile, so unless
            // --avoid-known-bugs is false a CASE has one WHEN
            int n = 1 + (!avoidKnownBugs && Randomly.getBooleanWithRatherLowProbability() ? 1 : 0);
            for (int i = 0; i < n; i++) {
                conditions.add(generateExpression(Kind.BOOLEAN, depth + 1));
                results.add(generateExpression(kind, depth + 1));
            }
            // ENG-30590 (comment): without an ELSE, a CASE with a DECIMAL literal result fails in the EE
            // ("unsupported type 'NUMERIC'"), so unless --avoid-known-bugs is false a CASE has an ELSE
            VoltDBExpression elseResult = !avoidKnownBugs && Randomly.getBooleanWithRatherLowProbability() ? null
                    : generateExpression(kind, depth + 1);
            return new VoltDBCase(conditions, results, elseResult);
        } finally {
            typedFunctionNesting--;
        }
    }

    // COALESCE of two or three expressions of the given kind
    private VoltDBExpression generateCoalesce(Kind kind, int depth) {
        List<VoltDBExpression> args = new ArrayList<>();
        typedFunctionNesting++;
        try {
            int n = 2 + (Randomly.getBooleanWithRatherLowProbability() ? 1 : 0);
            for (int i = 0; i < n; i++) {
                args.add(generateExpression(kind, depth + 1));
            }
            return new VoltDBFunction(args, VoltDBFunctionName.COALESCE);
        } finally {
            typedFunctionNesting--;
        }
    }

    private VoltDBExpression generateBooleanExpression(int depth) {
        if (canGenerateSubqueryPredicate() && Randomly.getBooleanWithRatherLowProbability()) {
            return generateSubqueryPredicate(depth);
        }
        switch (Randomly.fromOptions(BooleanExpression.values())) {
        case COMPARISON:
            Kind kind = getRandomType();
            VoltDBBinaryOperator op = Randomly.fromOptions(VoltDBBinaryOperator.EQUALS, VoltDBBinaryOperator.NOT_EQUALS,
                    VoltDBBinaryOperator.SMALLER, VoltDBBinaryOperator.SMALLER_EQUALS, VoltDBBinaryOperator.GREATER,
                    VoltDBBinaryOperator.GREATER_EQUALS);
            if (op.isOrdering() && !canCompareOrdered(kind)) {
                op = Randomly.fromOptions(VoltDBBinaryOperator.EQUALS, VoltDBBinaryOperator.NOT_EQUALS);
            }
            if (kind == Kind.VARBINARY && avoidKnownBugs) {
                // ENG-12656: comparing a VARBINARY(n) column that leads a multi-column or unique index with a longer
                // value fails, and the longer value can be another table's column; one side is a short constant
                return new VoltDBBinaryOperation(generateComparand(kind, depth), generateConstant(kind, false), op);
            }
            VoltDBBinaryOperator comparison = op;
            return inDomain(kind, () -> {
                if (kind == Kind.NUMERIC && comparison.isOrdering() && avoidKnownBugs) {
                    // ENG-30511: an index range scan on a narrower integer column with a key from a wider column that
                    // is below the narrower type's range matches every row; constant keys are handled correctly
                    return new VoltDBBinaryOperation(generateComparand(kind, depth), generateRangeBound(kind),
                            comparison);
                }
                return new VoltDBBinaryOperation(generateComparand(kind, depth), generateComparand(kind, depth),
                        comparison);
            });
        case AND_OR:
            return new VoltDBBinaryOperation(generateExpression(Kind.BOOLEAN, depth + 1),
                    generateExpression(Kind.BOOLEAN, depth + 1),
                    Randomly.fromOptions(VoltDBBinaryOperator.AND, VoltDBBinaryOperator.OR));
        case NOT:
            VoltDBExpression negated = generateExpression(Kind.BOOLEAN, depth + 1);
            if (avoidKnownBugs && negated instanceof VoltDBBinaryOperation && ((VoltDBBinaryOperation) negated)
                    .getOperatorRepresentation().equals(VoltDBBinaryOperator.OR.getTextRepresentation())) {
                // ENG-30504: the planner fails on NOT (b OR c) over BOOLEAN values when it is an operand, as in
                // NOT (NOT (b OR c)), (NOT (b OR c)) IS NOT NULL or (NOT (b OR c)) = TRUE
                return negated;
            }
            return not(negated);
        case IS_NULL:
            return generateIsNull(depth);
        case BETWEEN:
            Kind betweenKind;
            do {
                betweenKind = getRandomType();
            } while (!canCompareOrdered(betweenKind));
            if (avoidKnownBugs) {
                // ENG-30511 as for comparisons, and ENG-30512: COUNT(*) over an empty index range, such as
                // x BETWEEN 9 AND 1, is negative; the bounds are constants in ascending order
                Kind boundsKind = betweenKind;
                return inDomain(boundsKind, () -> {
                    VoltDBConstant low = generateRangeBound(boundsKind);
                    VoltDBConstant high = generateRangeBound(boundsKind);
                    if (compareConstants(boundsKind, low, high) > 0) {
                        VoltDBConstant swap = low;
                        low = high;
                        high = swap;
                    }
                    return new VoltDBBetweenOperation(generateExpression(boundsKind, depth + 1), low, high,
                            Randomly.getBoolean());
                });
            }
            return new VoltDBBetweenOperation(generateExpression(betweenKind, depth + 1),
                    generateExpression(betweenKind, depth + 1), generateExpression(betweenKind, depth + 1),
                    Randomly.getBoolean());
        case IN:
            if (comparandNesting > 0) {
                // VoltDB's parser rejects an IN predicate inside an operand of a comparison or IN, such as
                // x = (y IN (1)): "unexpected token: IN"
                return generateIsNull(depth);
            }
            if (hasOuterJoin && avoidKnownBugs) {
                // ENG-30507: with an outer join, an IN predicate on an indexed column can be applied as part of the
                // join instead of after it, so it does not remove the rows it should
                return generateIsNull(depth);
            }
            Kind inKind = getRandomType();
            return inDomain(inKind, () -> {
                List<VoltDBExpression> list = new ArrayList<>();
                int n = 1 + Randomly.smallNumber();
                for (int i = 0; i < n; i++) {
                    list.add(generateInListElement(inKind, depth));
                }
                VoltDBExpression left = generateComparand(inKind, depth);
                if (avoidKnownBugs && left instanceof VoltDBSubquery) {
                    // ENG-30596: a bare scalar subquery on the left of an IN list fails in the EE ("Invalid
                    // ExpressionType 'COMPARE_IN' called for VectorComparisonExpression"), so unless --avoid-known-bugs
                    // is false a numeric one is written (s + 0), and a string one is not generated: s || '' would be
                    // NULL for '' (ENG-30546)
                    left = inKind == Kind.NUMERIC
                            ? new VoltDBBinaryOperation(left, new VoltDBConstant("0"), VoltDBBinaryOperator.ADD)
                            : withoutSubqueries(() -> generateComparand(inKind, depth));
                }
                return new VoltDBInOperation(left, list, Randomly.getBoolean());
            });
        case LIKE:
            return generateLike(depth);
        default:
            throw new AssertionError();
        }
    }

    // An operand of a comparison or IN. ENG-30497: VoltDB turns an outer join into an inner join when the WHERE clause
    // compares a boolean expression over the inner table, such as (t.c IS NULL) = TRUE, with something, although the
    // comparison need not reject NULLs. With an outer join in the FROM clause, BOOLEAN operands are therefore only
    // columns and constants, unless --avoid-known-bugs is false.
    private VoltDBExpression generateComparand(Kind kind, int depth) {
        if (kind == Kind.BOOLEAN && hasOuterJoin && avoidKnownBugs) {
            return generateLeafNode(kind);
        }
        comparandNesting++;
        try {
            return generateExpression(kind, depth + 1);
        } finally {
            comparandNesting--;
        }
    }

    // ENG-30503: VoltDB ignores NULL elements of an IN list, so x NOT IN (NULL, ...) is TRUE instead of NULL
    // (ENG-30500 is another wrong result with a NULL in the list). Unless --avoid-known-bugs is false, IN list
    // elements are constants that are not NULL.
    private VoltDBExpression generateInListElement(Kind kind, int depth) {
        if (!avoidKnownBugs) {
            return generateComparand(kind, depth);
        }
        return generateConstant(kind, false);
    }

    // ENG-30513: comparing a FLOAT with a DECIMAL of magnitude 2^63 or more gives wrong results, and such DECIMALs come
    // from the data and from arithmetic, not only constants. Unless --avoid-known-bugs is false, a numeric comparison,
    // BETWEEN or IN draws all its operands from either FLOAT or the exact types (integers and DECIMAL).
    // ENG-30545: comparing a DATE with a TIMESTAMP truncates the TIMESTAMP to its date when the DATE is the left
    // operand, so unless --avoid-known-bugs is false, a temporal comparison, BETWEEN or IN draws all its operands from
    // either DATE or TIMESTAMP. ENG-30560: a DATE constant in a statement makes its DECIMAL literals FLOAT and its
    // folded DECIMAL constants fail to compile, and a comparison needs constants, so that is TIMESTAMP.
    private VoltDBExpression inDomain(Kind kind, Supplier<VoltDBExpression> generator) {
        if (kind == Kind.TEMPORAL && avoidKnownBugs && temporalType == null) {
            temporalType = VoltDBDataType.TIMESTAMP;
            try {
                return generator.get();
            } finally {
                temporalType = null;
            }
        }
        if (kind != Kind.NUMERIC || !avoidKnownBugs || numericDomain != NumericDomain.ANY) {
            return generator.get();
        }
        numericDomain = Randomly.fromOptions(NumericDomain.EXACT, NumericDomain.FLOAT);
        try {
            return generator.get();
        } finally {
            numericDomain = NumericDomain.ANY;
        }
    }

    // An expression of the given kind whose operands come from one numeric or temporal domain, as a comparison's do,
    // for a SELECT list
    public VoltDBExpression generateExpressionInDomain(Kind kind) {
        return inDomain(kind, () -> generateExpression(kind));
    }

    private boolean isUsable(VoltDBColumn c, Kind kind) {
        VoltDBDataType type = c.getType().getType();
        return Kind.fromType(type) == kind && (kind != Kind.NUMERIC || numericDomain.contains(type))
                && (kind != Kind.TEMPORAL || temporalType == null || type == temporalType);
    }

    // A constant bound for an ordered comparison or BETWEEN. ENG-30515: INDEX COUNT clamps a bound outside the
    // column type's range to the type's limit and then misses rows holding that limit (TINYINT c < 32767 does not
    // count 127), so numeric bounds lie inside every integer type's range: -126 to 126. ENG-30521: exact bounds have no
    // fraction unless --avoid-known-bugs is false, since a DECIMAL literal compares wrongly with a DECIMAL expression
    // of
    // 2^63 or more.
    private VoltDBConstant generateRangeBound(Kind kind) {
        if (kind != Kind.NUMERIC) {
            return (VoltDBConstant) generateConstant(kind, false);
        }
        Randomly r = state.getRandomly();
        long whole = r.getLong(-126, 127);
        String text;
        if (numericDomain == NumericDomain.FLOAT) {
            text = whole + "." + r.getInteger(0, 1000) + "E0";
        } else if (Randomly.getBoolean() && !avoidKnownBugs) {
            text = whole + "." + r.getInteger(0, 1000);
        } else {
            text = Long.toString(whole);
        }
        return new VoltDBConstant(whole < 0 ? "(" + text + ")" : text);
    }

    // Orders two NUMERIC or TEMPORAL constants as generateConstant writes them: a number, possibly in parentheses, or
    // CAST('uuuu-MM-dd HH:mm:ss.SSSSSS' AS TIMESTAMP), whose text orders as the timestamp does.
    private static int compareConstants(Kind kind, VoltDBConstant a, VoltDBConstant b) {
        if (kind == Kind.NUMERIC) {
            return new BigDecimal(a.toString().replaceAll("[()]", ""))
                    .compareTo(new BigDecimal(b.toString().replaceAll("[()]", "")));
        }
        return a.toString().compareTo(b.toString());
    }

    // ENG-30497: with an outer join, (x IS NULL) IS NOT NULL and the like over the inner table turn the join into an
    // inner join, so the operand is a column or a constant unless --avoid-known-bugs is false
    // ENG-30516: WHERE c IS NULL AND c = 5 on an indexed column loses the IS NULL when c = 5 becomes the index lookup,
    // so unless --avoid-known-bugs is false, IS NULL is written NOT (x IS NOT NULL), which VoltDB keeps.
    private VoltDBExpression generateIsNull(int depth) {
        Kind kind = getRandomType();
        VoltDBExpression operand = hasOuterJoin && avoidKnownBugs ? generateLeafNode(kind)
                : withoutNot(generateExpression(kind, depth + 1));
        VoltDBUnaryPostfixOperator op = Randomly.fromOptions(VoltDBUnaryPostfixOperator.values());
        if (op == VoltDBUnaryPostfixOperator.IS_NULL && avoidKnownBugs) {
            return new VoltDBUnaryPrefixOperation(
                    new VoltDBUnaryPostfixOperation(operand, VoltDBUnaryPostfixOperator.IS_NOT_NULL),
                    VoltDBUnaryPrefixOperator.NOT);
        }
        return new VoltDBUnaryPostfixOperation(operand, op);
    }

    // ENG-30493: VoltDB orders BOOLEAN values TRUE < FALSE where it evaluates them, so <, >, BETWEEN and ORDER BY on
    // BOOLEAN values give wrong results; they are left out unless --avoid-known-bugs is false.
    private boolean canOrder(Kind kind) {
        return kind != Kind.BOOLEAN || !avoidKnownBugs;
    }

    // ENG-30509 and ENG-12656: an index range scan on a VARCHAR(n) or VARBINARY(n) column compares it with only the
    // first n characters of a longer value (wrong rows), or fails for VARBINARY. The longer value can be another
    // table's column, so short constants do not avoid it: <, >, <=, >= and BETWEEN on strings and binary values are
    // left out unless --avoid-known-bugs is false. ORDER BY is not affected.
    private boolean canCompareOrdered(Kind kind) {
        return canOrder(kind) && (!avoidKnownBugs || kind != Kind.STRING && kind != Kind.VARBINARY);
    }

    private VoltDBExpression generateNumericExpression(int depth) {
        NumericExpression choice = Randomly.fromOptions(NumericExpression.values());
        switch (choice) {
        case UNARY_MINUS:
            if (avoidKnownBugs) {
                // ENG-30502: unary minus on a NULL integer or FLOAT gives a non-NULL value; 0 - x does not
                return new VoltDBBinaryOperation(new VoltDBConstant("0"), generateExpression(Kind.NUMERIC, depth + 1),
                        VoltDBBinaryOperator.SUBTRACT);
            }
            return new VoltDBUnaryPrefixOperation(generateExpression(Kind.NUMERIC, depth + 1),
                    VoltDBUnaryPrefixOperator.MINUS);
        case ARITHMETIC:
            VoltDBBinaryOperator op = Randomly.fromOptions(VoltDBBinaryOperator.ADD, VoltDBBinaryOperator.SUBTRACT,
                    VoltDBBinaryOperator.MULTIPLY, VoltDBBinaryOperator.DIVIDE);
            if ((op == VoltDBBinaryOperator.MULTIPLY || op == VoltDBBinaryOperator.DIVIDE) && typedFunctionNesting > 0
                    && avoidKnownBugs) {
                // ENG-30550: ABS pushes its argument's type into the operands, so an integer division inside it keeps a
                // fraction when the argument is DECIMAL or FLOAT, as in ABS((7 / i) - d), and a BIGINT multiplication
                // makes ABS DECIMAL, as in ABS(b * 1) / 2 = 3.5. FLOOR, CEILING, MOD, CASE and COALESCE do the same
                // (comment on the ticket): there is no multiplication or division inside them
                op = Randomly.fromOptions(VoltDBBinaryOperator.ADD, VoltDBBinaryOperator.SUBTRACT);
            }
            return new VoltDBBinaryOperation(generateExpression(Kind.NUMERIC, depth + 1),
                    generateExpression(Kind.NUMERIC, depth + 1), op);
        case ABS:
        case FLOOR_CEILING:
            VoltDBFunctionName name = choice == NumericExpression.ABS ? VoltDBFunctionName.ABS
                    : Randomly.fromOptions(VoltDBFunctionName.FLOOR, VoltDBFunctionName.CEILING);
            typedFunctionNesting++;
            try {
                return new VoltDBFunction(List.of(generateExpression(Kind.NUMERIC, depth + 1)), name);
            } finally {
                typedFunctionNesting--;
            }
        case MOD:
            // MOD takes integers only ("incompatible data type in operation"); its result is an integer
            if (numericDomain == NumericDomain.FLOAT) {
                return generateLeafNode(Kind.NUMERIC);
            }
            typedFunctionNesting++;
            try {
                VoltDBExpression divisor = Randomly.getBoolean() ? generateIntegerOperand()
                        : integerConstant(Randomly.fromOptions(-7L, -3L, -2L, 2L, 3L, 5L, 10L));
                return new VoltDBFunction(List.of(generateIntegerOperand(), divisor), VoltDBFunctionName.MOD);
            } finally {
                typedFunctionNesting--;
            }
        case CHAR_LENGTH:
            // an integer, the length in characters
            if (numericDomain == NumericDomain.FLOAT) {
                return generateLeafNode(Kind.NUMERIC);
            }
            return new VoltDBFunction(List.of(generateExpression(Kind.STRING, depth + 1)),
                    VoltDBFunctionName.CHAR_LENGTH);
        case CASE:
            return generateCase(Kind.NUMERIC, depth);
        case COALESCE:
            return generateCoalesce(Kind.NUMERIC, depth);
        case FLOAT_MATH:
            // FLOAT results; a result that is NaN or infinite is "Invalid result value", expected
            if (numericDomain == NumericDomain.EXACT) {
                return generateLeafNode(Kind.NUMERIC);
            }
            VoltDBFunctionName math = Randomly.fromOptions(VoltDBFunctionName.SQRT, VoltDBFunctionName.EXP,
                    VoltDBFunctionName.LN, VoltDBFunctionName.LOG10, VoltDBFunctionName.POWER, VoltDBFunctionName.SIN,
                    VoltDBFunctionName.COS);
            // ENG-30550 (see ARITHMETIC): no multiplication or division inside, as in ABS
            typedFunctionNesting++;
            try {
                List<VoltDBExpression> mathArgs = new ArrayList<>();
                mathArgs.add(generateExpression(Kind.NUMERIC, depth + 1));
                if (math == VoltDBFunctionName.POWER) {
                    mathArgs.add(integerConstant(state.getRandomly().getInteger(-3, 4)));
                }
                return new VoltDBFunction(mathArgs, math);
            } finally {
                typedFunctionNesting--;
            }
        case DATE_PART:
            if (numericDomain == NumericDomain.FLOAT) {
                return generateLeafNode(Kind.NUMERIC);
            }
            VoltDBFunctionName part = Randomly.fromOptions(VoltDBFunctionName.YEAR, VoltDBFunctionName.MONTH,
                    VoltDBFunctionName.DAY, VoltDBFunctionName.HOUR, VoltDBFunctionName.MINUTE,
                    VoltDBFunctionName.DAYOFWEEK, VoltDBFunctionName.DAYOFYEAR, VoltDBFunctionName.QUARTER,
                    VoltDBFunctionName.WEEK, VoltDBFunctionName.WEEKDAY);
            // a DATE has no hour or minute: HOUR(d) and MINUTE(d) are "incompatible data type in conversion"
            boolean timeOfDay = part == VoltDBFunctionName.HOUR || part == VoltDBFunctionName.MINUTE;
            return new VoltDBFunction(
                    List.of(timeOfDay ? generateTimestamp(depth) : generateExpression(Kind.TEMPORAL, depth + 1)), part);
        case SINCE_EPOCH:
            if (numericDomain == NumericDomain.FLOAT) {
                return generateLeafNode(Kind.NUMERIC);
            }
            return new VoltDBFunction(
                    List.of(new VoltDBKeyword(Randomly.fromOptions("SECOND", "MILLISECOND", "MICROSECOND")),
                            generateTimestamp(depth)),
                    VoltDBFunctionName.SINCE_EPOCH);
        case JSON_ARRAY_LENGTH:
            if (numericDomain == NumericDomain.FLOAT) {
                return generateLeafNode(Kind.NUMERIC);
            }
            return new VoltDBFunction(List.of(generateJsonArray(depth)), VoltDBFunctionName.ARRAY_LENGTH);
        default:
            throw new AssertionError();
        }
    }

    // An integer column of the FROM clause, or an integer constant: an operand of MOD
    private VoltDBExpression generateIntegerOperand() {
        List<VoltDBColumn> integers = columns.stream().filter(c -> isUsable(c, Kind.NUMERIC)).filter(c -> {
            VoltDBDataType t = c.getType().getType();
            return t == VoltDBDataType.TINYINT || t == VoltDBDataType.SMALLINT || t == VoltDBDataType.INTEGER
                    || t == VoltDBDataType.BIGINT;
        }).collect(Collectors.toList());
        if (!integers.isEmpty() && Randomly.getBoolean()) {
            return new VoltDBColumnReference(Randomly.fromList(integers));
        }
        return integerConstant(state.getRandomly().getInteger(-1000, 1000));
    }

    // ENG-30496: inside CASE WHEN, which is where NoREC's unoptimized query puts the predicate, VoltDB's planner
    // crashes on a LIKE with a constant left operand, or with a pattern that has no wildcard or is just '%'. Unless
    // --avoid-known-bugs is false, the left operand is a string column and the pattern has a wildcard and a literal
    // character.
    private VoltDBExpression generateLike(int depth) {
        VoltDBBinaryOperator op = Randomly.fromOptions(VoltDBBinaryOperator.LIKE, VoltDBBinaryOperator.NOT_LIKE);
        if (!avoidKnownBugs) {
            return new VoltDBBinaryOperation(generateExpression(Kind.STRING, depth + 1),
                    Randomly.getBoolean() ? generateLikePattern() : generateExpression(Kind.STRING, depth + 1), op);
        }
        if (!canGenerateColumnOfType(Kind.STRING)) {
            return generateIsNull(depth);
        }
        return new VoltDBBinaryOperation(generateColumn(Kind.STRING), generateLikePattern(), op);
    }

    // A LIKE pattern: a random string with wildcards mixed in, keeping at least one wildcard and one other character.
    private VoltDBExpression generateLikePattern() {
        String s = state.getRandomly().getString().replace("%", "").replace("_", "");
        if (s.isEmpty()) {
            s = state.getRandomly().getAlphabeticChar();
        }
        StringBuilder sb = new StringBuilder();
        boolean wildcard = false;
        for (int i = 0; i < s.length(); i++) {
            // ENG-30509: VoltDB scans an index for the literal prefix of the pattern, which must be at most one
            // character (see generateConstant)
            if (i == 1 && avoidKnownBugs && !wildcard || Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(Randomly.fromOptions("%", "_"));
                wildcard = true;
            }
            sb.append(s.charAt(i));
        }
        if (!wildcard || Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("%", "_"));
        }
        return new VoltDBConstant(VoltDBConstantGenerator.quote(sb.toString()));
    }

    @Override
    public VoltDBExpression generateConstant(Kind kind) {
        return generateConstant(kind, true);
    }

    private VoltDBExpression generateConstant(Kind kind, boolean mayBeNull) {
        VoltDBDataType type = randomDataType(kind);
        if (kind == Kind.TEMPORAL && avoidKnownBugs) {
            // ENG-30499: the planner crashes folding a comparison of a DATE constant with a TIMESTAMP constant, and
            // ENG-30560 makes a statement's DECIMAL literals FLOAT when it holds a DATE constant: constants are
            // TIMESTAMPs
            type = VoltDBDataType.TIMESTAMP;
        }
        // ENG-30495: a WHERE clause that folds to a constant NULL crashes the planner, and only NULL literals can make
        // it fold to NULL (ENG-30500 and ENG-30503 are wrong results with NULLs in IN lists). Unless
        // --avoid-known-bugs is false, queries get their NULLs from the data only.
        if (mayBeNull && !avoidKnownBugs && Randomly.getBooleanWithRatherLowProbability()) {
            // VoltDB needs a typed NULL: a bare NULL is "data type cast needed for parameter or null literal"
            return new VoltDBConstant("CAST(NULL AS " + type + ")");
        }
        if (kind == Kind.BOOLEAN && avoidKnownBugs) {
            // ENG-30494: VoltDB passes BOOLEAN literals to the EE as parameters of the wrong value or type, giving
            // cast errors and wrong rows; a comparison of integers is a boolean constant without a BOOLEAN literal
            return new VoltDBConstant(Randomly.getBoolean() ? "(1 = 1)" : "(1 = 0)");
        }
        if (kind == Kind.STRING && avoidKnownBugs) {
            // ENG-30509: a range scan on a multi-column index compares a VARCHAR(n) column with only the first n
            // characters of a longer value, and ENG-30505 is an error for IN on a unique index; the smallest generated
            // VARCHAR column holds one character
            String str = state.getRandomly().getString();
            if (str.codePointCount(0, str.length()) > 1) {
                str = str.substring(0, str.offsetByCodePoints(0, 1));
            }
            return new VoltDBConstant(VoltDBConstantGenerator.quote(str));
        }
        if (kind == Kind.VARBINARY && avoidKnownBugs) {
            // ENG-12656: an index scan for <, <=, >, >= or BETWEEN on a VARBINARY(n) column fails ("Type VARCHAR can't
            // be cast as VARBINARY") when the value is longer than n bytes, and ENG-30505 is the same for IN on a
            // unique
            // index; the smallest generated VARBINARY column holds one byte
            byte[] bytes = state.getRandomly().getBytes();
            if (bytes.length == 0 && inAggregate) {
                // ENG-30534: aggregates of x'' and of '' in a GROUP BY join whose ON condition holds x'' fail with a
                // cast error, as if the two were one expression; an aggregate's VARBINARY constants are not empty
                bytes = new byte[] { 0 };
            }
            return new VoltDBConstant(bytes.length == 0 ? "x''" : String.format("x'%02X'", bytes[0]));
        }
        String literal = VoltDBConstantGenerator.generate(state.getRandomly(), VoltDBCompositeDataType.getRandom(type));
        if (kind == Kind.TEMPORAL) {
            // a string literal compares with TIMESTAMP and DATE values, but not inside an IN list
            return new VoltDBConstant("CAST(" + literal + " AS " + type + ")");
        }
        if (literal.startsWith("-")) {
            // VoltDB's parser rejects "- -5", which the unary minus would otherwise produce
            return new VoltDBConstant("(" + literal + ")");
        }
        return new VoltDBConstant(literal);
    }

    // a one-character VARCHAR constant (ENG-30509, see generateConstant)
    private VoltDBExpression generateNonEmptyStringConstant() {
        String str = state.getRandomly().getString();
        str = str.isEmpty() ? state.getRandomly().getAlphabeticChar() : str.substring(0, str.offsetByCodePoints(0, 1));
        return new VoltDBConstant(VoltDBConstantGenerator.quote(str));
    }

    private VoltDBDataType randomDataType(Kind kind) {
        switch (kind) {
        case BOOLEAN:
            return VoltDBDataType.BOOLEAN;
        case NUMERIC:
            VoltDBDataType type;
            // ENG-30521: when a join's statement keeps its literals inline, a DECIMAL expression of 2^63 or more
            // compares wrongly with any DECIMAL literal (a non-integer, or an integer outside BIGINT's range), so
            // unless --avoid-known-bugs is false the exact side of a comparison, BETWEEN or IN has integer literals
            do {
                type = Randomly.fromOptions(VoltDBDataType.TINYINT, VoltDBDataType.SMALLINT, VoltDBDataType.INTEGER,
                        VoltDBDataType.BIGINT, VoltDBDataType.FLOAT, VoltDBDataType.DECIMAL);
            } while (!numericDomain.contains(type)
                    || type == VoltDBDataType.DECIMAL && numericDomain == NumericDomain.EXACT && avoidKnownBugs);
            return type;
        case STRING:
            return VoltDBDataType.VARCHAR;
        case VARBINARY:
            return VoltDBDataType.VARBINARY;
        case TEMPORAL:
            return Randomly.fromOptions(VoltDBDataType.TIMESTAMP, VoltDBDataType.DATE);
        default:
            throw new AssertionError(kind);
        }
    }

    @Override
    protected VoltDBExpression generateColumn(Kind kind) {
        List<VoltDBColumn> usable = columns.stream().filter(c -> isUsable(c, kind)).collect(Collectors.toList());
        if (aggregateColumns != null && canAggregate(kind) && (usable.isEmpty() || Randomly.getBoolean())) {
            return generateAggregate(kind);
        }
        return new VoltDBColumnReference(Randomly.fromList(usable));
    }

    // MIN and MAX of BOOLEAN values are left out: VoltDB orders them TRUE < FALSE (ENG-30493). COUNT is exact, and
    // SUM, MIN and MAX of a FLOAT expression always exist, since an expression can be a constant.
    private static boolean canAggregate(Kind kind) {
        return kind != Kind.BOOLEAN;
    }

    // An aggregate of the given kind over the FROM clause's columns (or, inside a HAVING clause, over aggregateColumns)
    public VoltDBExpression generateAggregate(Kind kind) {
        List<VoltDBAggregateFunction> functions = new ArrayList<>(
                List.of(VoltDBAggregateFunction.MIN, VoltDBAggregateFunction.MAX));
        if (kind == Kind.NUMERIC) {
            functions.add(VoltDBAggregateFunction.SUM);
            if (numericDomain != NumericDomain.FLOAT) {
                functions.add(VoltDBAggregateFunction.COUNT);
            }
        }
        VoltDBAggregateFunction function = Randomly.fromList(functions);
        List<VoltDBColumn> groupingColumns = columns;
        List<VoltDBColumn> outerAggregateColumns = aggregateColumns;
        if (aggregateColumns != null) {
            columns = aggregateColumns;
            aggregateColumns = null; // no aggregate inside an aggregate
        }
        inAggregate = true;
        try {
            List<VoltDBExpression> args;
            if (function != VoltDBAggregateFunction.COUNT) {
                VoltDBExpression arg = generateExpression(kind, 1);
                if (avoidsMinMaxShortcut() && arg instanceof VoltDBColumnReference) {
                    arg = kind == Kind.STRING
                            ? new VoltDBBinaryOperation(arg, new VoltDBConstant("''"), VoltDBBinaryOperator.CONCAT)
                            : new VoltDBBinaryOperation(arg, new VoltDBConstant("0"), VoltDBBinaryOperator.ADD);
                }
                args = List.of(arg);
            } else if (Randomly.getBoolean()) {
                args = List.of(); // COUNT(*)
            } else {
                // a COUNT is exact whatever it counts, so its argument may be FLOAT in an exact comparison
                NumericDomain domain = numericDomain;
                numericDomain = NumericDomain.ANY;
                try {
                    args = List.of(generateExpression(getRandomType(), 1));
                } finally {
                    numericDomain = domain;
                }
            }
            aggregated = true;
            return new VoltDBAggregate(args, function);
        } finally {
            inAggregate = false;
            columns = groupingColumns;
            aggregateColumns = outerAggregateColumns;
        }
    }

    @Override
    protected Kind getRandomType() {
        return Randomly.fromOptions(Kind.values());
    }

    @Override
    protected boolean canGenerateColumnOfType(Kind kind) {
        return columns.stream().anyMatch(c -> isUsable(c, kind)) || aggregateColumns != null && canAggregate(kind);
    }

    @Override
    public VoltDBExpressionGenerator setTablesAndColumns(AbstractTables<VoltDBTable, VoltDBColumn> tables) {
        fromTables = new ArrayList<>();
        joins = new ArrayList<>();
        hasOuterJoin = false;
        List<VoltDBTable> partitionJoined = new ArrayList<>();
        VoltDBTable anchor = null;
        List<VoltDBTable> candidates = tables.getTables();
        if (avoidKnownBugs) {
            // ENG-30582: once a table has a materialized view, VoltDB's view rewriter fails to plan some GROUP BY and
            // aggregate queries on it ("Duplicate key", ClassCastException), so queries leave such tables to the VIEWS
            // oracle
            Set<String> viewed = state.getViews().values().stream().map(v -> v[0]).collect(Collectors.toSet());
            candidates = candidates.stream().filter(t -> !viewed.contains(t.getName())).collect(Collectors.toList());
            if (candidates.isEmpty()) {
                throw new IgnoreMeException();
            }
        }
        for (VoltDBTable t : candidates) {
            if (!t.isPartitioned()) {
                fromTables.add(t);
            } else if (anchor == null) {
                anchor = t;
            } else if (canPartitionJoin(anchor, t)) {
                partitionJoined.add(t);
            }
            // a partitioned table whose partitioning column cannot be compared with the anchor's is left out
        }
        replicatedOnly = anchor == null;
        partitionAnchor = anchor;
        if (anchor == null) {
            anchor = fromTables.remove(fromTables.size() - 1);
        }
        List<VoltDBTable> explicitlyJoined = new ArrayList<>();
        // with partitioned tables joined, a LEFT JOIN of a replicated table whose ON condition folds to FALSE is "not
        // plannable" once a WHERE clause on that table makes it an inner join, and a FULL JOIN is left out for
        // ENG-30531, so such a generator keeps replicated tables in the FROM list
        boolean mayJoinReplicated = !(avoidKnownBugs && noFullJoins && !partitionJoined.isEmpty());
        for (VoltDBTable t : new ArrayList<>(fromTables)) {
            if (mayJoinReplicated && Randomly.getBooleanWithRatherLowProbability()) {
                fromTables.remove(t);
                explicitlyJoined.add(t);
            }
        }
        fromTables.add(anchor);

        List<VoltDBColumn> all = new ArrayList<>();
        fromTables.forEach(t -> all.addAll(t.getColumns()));
        for (VoltDBTable t : partitionJoined) {
            joins.add(new VoltDBJoin(new VoltDBTableReference(t), randomJoinType(), partitionJoinCondition(anchor, t)));
            all.addAll(t.getColumns());
        }
        for (VoltDBTable t : explicitlyJoined) {
            // a chain of INNER JOINs with partitioned tables, where a replicated table's ON condition folds to FALSE,
            // always loses the partitioning columns' equality ("This query is not plannable"); an outer join loses it
            // only with some WHERE clauses, and the error is expected (VoltDBErrors)
            VoltDBJoin.JoinType type = partitionJoined.isEmpty() ? randomJoinType()
                    : Randomly.fromOptions(VoltDBJoin.JoinType.LEFT, VoltDBJoin.JoinType.FULL);
            joins.add(new VoltDBJoin(new VoltDBTableReference(t), type, joinCondition(anchor, t, type)));
            all.addAll(t.getColumns());
        }
        this.columns = all;
        hasOuterJoin = joins.stream().anyMatch(j -> j.getJoinType() != VoltDBJoin.JoinType.INNER);
        subqueryAliases = 0;
        if (subqueries) {
            // ENG-30582 (above) applies to the aggregates of scalar subqueries too
            Set<String> viewed = avoidKnownBugs
                    ? state.getViews().values().stream().map(v -> v[0]).collect(Collectors.toSet()) : Set.of();
            subqueryTables = state.getSchema().getDatabaseTables().stream()
                    .filter(t -> !t.isPartitioned() && !viewed.contains(t.getName())).collect(Collectors.toList());
        }
        return this;
    }

    // Subqueries in the expressions this generator makes: EXISTS, IN, ANY and ALL predicates, and scalar subqueries
    // that aggregate, correlated with the outer query or not. VoltDB runs subqueries over replicated tables only, and
    // rejects them in a HAVING clause, in an aggregate's argument and in a window's ORDER BY, so there are none there.
    // Generators with subqueries off make the same choices as before subqueries existed.
    public VoltDBExpressionGenerator setSubqueries(boolean subqueries) {
        this.subqueries = subqueries;
        return this;
    }

    private boolean canGenerateSubquery() {
        return subqueries && !subqueryTables.isEmpty() && subqueryNesting < MAX_SUBQUERY_NESTING
                && aggregateColumns == null && !inAggregate && subqueriesBlocked == 0;
    }

    // ENG-30497 (comment): VoltDB takes NOT IN, <> ALL and NOT EXISTS over an outer join's inner table to reject NULLs
    // and turns the join into an inner join, though they can be TRUE for the null-extended row; so unless
    // --avoid-known-bugs is false, there are no subquery predicates with an outer join
    private boolean canGenerateSubqueryPredicate() {
        return canGenerateSubquery() && !(hasOuterJoin && avoidKnownBugs);
    }

    private <T> T withoutSubqueries(Supplier<T> body) {
        subqueriesBlocked++;
        try {
            return body.get();
        } finally {
            subqueriesBlocked--;
        }
    }

    // An expression of the given kind in one domain (see generateExpressionInDomain) without subqueries, for a window
    // function's argument: "Window function calls with subquery expression arguments are not allowed"
    public VoltDBExpression generateWindowArgument(Kind kind) {
        return withoutSubqueries(() -> generateExpressionInDomain(kind));
    }

    // ENG-30533 (see setUngroupedAggregates): a subquery's MIN or MAX has no GROUP BY, so unless --avoid-known-bugs is
    // false its argument is a NUMERIC or STRING expression, which (c + 0) and (c || '') keep from being a bare column
    // ENG-30628: an outer join whose WHERE clause compares a scalar subquery with a partitioned table's primary key
    // fails to plan (IllegalStateException in SubPlanAssembler.buildIndexAccessPlanForTable), so unless
    // --avoid-known-bugs is false, there are no scalar subqueries with an outer join
    private boolean canGenerateScalarSubquery(Kind kind) {
        return canGenerateSubquery()
                && (!avoidKnownBugs || (kind == Kind.NUMERIC || kind == Kind.STRING) && !hasOuterJoin);
    }

    // A copy of the table under the next alias, S0, S1, ..., whose columns a subquery's expressions reference
    private VoltDBTable aliasOf(VoltDBTable t) {
        List<VoltDBColumn> copies = t.getColumns().stream()
                .map(c -> new VoltDBColumn(c.getName(), c.getType(), c.isNullable())).collect(Collectors.toList());
        VoltDBTable alias = new VoltDBTable("S" + subqueryAliases++, copies, null, List.of());
        copies.forEach(c -> c.setTable(alias));
        return alias;
    }

    // Generates inside a subquery over alias: with only its columns in scope, or with the outer query's too
    private <T> T inSubquery(VoltDBTable alias, boolean correlated, Supplier<T> body) {
        List<VoltDBColumn> outer = columns;
        List<VoltDBColumn> scope = new ArrayList<>(alias.getColumns());
        if (correlated) {
            scope.addAll(outer);
        }
        columns = scope;
        subqueryNesting++;
        try {
            return body.get();
        } finally {
            columns = outer;
            subqueryNesting--;
        }
    }

    // a subquery's WHERE condition, which may reference the outer query's columns when correlated, or null
    private VoltDBExpression generateSubqueryWhere(VoltDBTable alias, int depth) {
        return generateSubqueryWhere(alias, depth, true);
    }

    private VoltDBExpression generateSubqueryWhere(VoltDBTable alias, int depth, boolean correlated) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return null;
        }
        return inSubquery(alias, correlated, () -> generateExpression(Kind.BOOLEAN, depth + 1));
    }

    // EXISTS, IN, or a comparison with ANY or ALL. VoltDB's parser rejects IN inside an operand of a comparison or IN,
    // as for IN lists, and ANY and ALL with it, so there it is EXISTS.
    // ENG-30600: an IN or = ANY subquery inside another subquery, whose left operand is a column of the outermost
    // query, holds for every row; unless --avoid-known-bugs is false, a subquery inside a subquery is EXISTS.
    private VoltDBExpression generateSubqueryPredicate(int depth) {
        if (comparandNesting > 0 || subqueryNesting > 0 && avoidKnownBugs || Randomly.getBoolean()) {
            VoltDBTable table = Randomly.fromList(subqueryTables);
            VoltDBTable alias = aliasOf(table);
            return VoltDBSubquery.exists(table.getName(), alias, generateSubqueryWhere(alias, depth));
        }
        return generateSubqueryComparison(depth);
    }

    // x [NOT] IN (SELECT e ...) or x op ANY|ALL (SELECT e ...), x and e of one kind and domain. ENG-12656: an index
    // lookup on a VARBINARY(n) column with a longer value fails, and either side can be the indexed column, so unless
    // --avoid-known-bugs is false they are not VARBINARY.
    private VoltDBSubquery generateSubqueryComparison(int depth) {
        Kind kind;
        do {
            kind = getRandomType();
        } while (kind == Kind.VARBINARY && avoidKnownBugs);
        Kind k = kind;
        VoltDBTable table = Randomly.fromList(subqueryTables);
        return (VoltDBSubquery) inDomain(kind, () -> {
            // ENG-30595: a string function of a scalar subquery on the left of IN or = ANY gets a value of the wrong
            // type, and CHAR_LENGTH of it crashes the server, so unless --avoid-known-bugs is false the left operand
            // has no subquery
            VoltDBExpression left = avoidKnownBugs ? withoutSubqueries(() -> generateComparand(k, depth))
                    : generateComparand(k, depth);
            if (k == Kind.STRING && avoidKnownBugs && !referencesColumn(left)) {
                // ENG-30549: the planner folds a comparison of two string constants ignoring trailing spaces, while
                // the EE compares exactly; with a constant on both sides, the EXISTS form of the predicate (see
                // existsRewrite) is folded and the subquery form is not, so the left operand reads a column
                if (!canGenerateColumnOfType(Kind.STRING)) {
                    throw new IgnoreMeException();
                }
                left = generateColumn(Kind.STRING);
            }
            VoltDBTable alias = aliasOf(table);
            // ENG-30594: a subquery in the output of an IN or = ANY subquery crashes the planner (NullPointerException
            // on "m_subqueryNode"), so unless --avoid-known-bugs is false the output has none
            // ENG-30597: with GROUP BY and an aggregate, an ALL or IN subquery whose output is the outer query's column
            // fails (EE "invalid column type", planner IndexOutOfBoundsException), so unless --avoid-known-bugs is
            // false the output reads the subquery's own columns only
            boolean correlated = !avoidKnownBugs && Randomly.getBooleanWithRatherLowProbability();
            VoltDBExpression output = avoidKnownBugs
                    ? withoutSubqueries(() -> inSubquery(alias, correlated, () -> generateExpression(k, depth + 1)))
                    : inSubquery(alias, correlated, () -> generateExpression(k, depth + 1));
            // ENG-30601 (comment): a bound on an indexed column of the subquery's table, then an equality with an outer
            // column, loses the bound; the EXISTS form (existsRewrite) puts x op e next to the WHERE condition, so
            // unless --avoid-known-bugs is false the WHERE condition of an IN, ANY or ALL subquery is not correlated
            VoltDBExpression where = generateSubqueryWhere(alias, depth, !avoidKnownBugs);
            if (Randomly.getBoolean()) {
                return VoltDBSubquery.inSet(left, Randomly.getBoolean(), table.getName(), alias, output, where);
            }
            VoltDBBinaryOperator op = Randomly.fromOptions(VoltDBBinaryOperator.EQUALS, VoltDBBinaryOperator.NOT_EQUALS,
                    VoltDBBinaryOperator.SMALLER, VoltDBBinaryOperator.SMALLER_EQUALS, VoltDBBinaryOperator.GREATER,
                    VoltDBBinaryOperator.GREATER_EQUALS);
            if (op.isOrdering() && !canCompareOrdered(k)) {
                op = Randomly.fromOptions(VoltDBBinaryOperator.EQUALS, VoltDBBinaryOperator.NOT_EQUALS);
            }
            return VoltDBSubquery.quantified(left, op, Randomly.getBoolean(), table.getName(), alias, output, where);
        });
    }

    // An IN, ANY or ALL subquery predicate, at the top of a WHERE clause (see VoltDBSubqueryOracle)
    public VoltDBSubquery generateSubqueryComparison() {
        if (!canGenerateSubqueryPredicate()) {
            throw new IgnoreMeException();
        }
        return generateSubqueryComparison(0);
    }

    // The same predicate with EXISTS, where it holds (is TRUE) for the same rows. With c the comparison of x and e
    // (x = e for IN):
    // x IN S and x op ANY S hold when some row of S makes c TRUE: EXISTS (SELECT 1 ... WHERE q AND c);
    // x NOT IN S (x <> ALL S) and x op ALL S hold when no row of S makes c FALSE or NULL:
    // NOT EXISTS (SELECT 1 ... WHERE q AND (c IS NULL OR NOT c)).
    public VoltDBExpression existsRewrite(VoltDBSubquery s) {
        VoltDBBinaryOperator op;
        boolean all;
        if (s.getForm() == VoltDBSubquery.Form.IN) {
            op = s.isNegated() ? VoltDBBinaryOperator.NOT_EQUALS : VoltDBBinaryOperator.EQUALS;
            all = s.isNegated();
        } else if (s.getForm() == VoltDBSubquery.Form.QUANTIFIED) {
            op = s.getOperator();
            all = s.isAll();
        } else {
            throw new AssertionError(s.getForm());
        }
        VoltDBExpression c = new VoltDBBinaryOperation(s.getLeft(), s.getOutput(), op);
        VoltDBExpression condition = all ? new VoltDBBinaryOperation(isNull(c),
                new VoltDBUnaryPrefixOperation(c, VoltDBUnaryPrefixOperator.NOT), VoltDBBinaryOperator.OR) : c;
        // ENG-30601: in q AND x = e, an upper bound on e's indexed column in q is dropped when x = e becomes the
        // unique index lookup; with the comparison first it is kept, so unless --avoid-known-bugs is false the
        // comparison comes first
        VoltDBExpression where = s.getWhere() == null ? condition
                : avoidKnownBugs ? new VoltDBBinaryOperation(condition, s.getWhere(), VoltDBBinaryOperator.AND)
                        : new VoltDBBinaryOperation(s.getWhere(), condition, VoltDBBinaryOperator.AND);
        VoltDBExpression exists = VoltDBSubquery.exists(s.getTableName(), s.getAlias(), where);
        return all ? new VoltDBUnaryPrefixOperation(exists, VoltDBUnaryPrefixOperator.NOT) : exists;
    }

    // (SELECT agg FROM t AS s [WHERE q]): MIN or MAX of an expression over s's columns, or a COUNT(*)
    private VoltDBExpression generateScalarSubquery(Kind kind, int depth) {
        VoltDBTable table = Randomly.fromList(subqueryTables);
        VoltDBTable alias = aliasOf(table);
        List<VoltDBAggregateFunction> functions = new ArrayList<>(
                List.of(VoltDBAggregateFunction.MIN, VoltDBAggregateFunction.MAX));
        // ENG-30497 (comment): a COUNT(*) subquery correlated to an outer join's inner table is 0, not NULL, for the
        // null-extended row, but VoltDB takes a comparison with it to reject NULLs and turns the join into an inner
        // join; so unless --avoid-known-bugs is false, a scalar subquery with an outer join is a MIN or MAX
        if (kind == Kind.NUMERIC && numericDomain != NumericDomain.FLOAT && !(hasOuterJoin && avoidKnownBugs)) {
            functions.add(VoltDBAggregateFunction.COUNT);
        }
        VoltDBAggregateFunction function = Randomly.fromList(functions);
        VoltDBExpression where = generateSubqueryWhere(alias, depth);
        List<VoltDBExpression> args = List.of();
        if (function != VoltDBAggregateFunction.COUNT) {
            inAggregate = true;
            try {
                VoltDBExpression arg = inSubquery(alias, false, () -> generateExpression(kind, depth + 1));
                if (avoidKnownBugs && arg instanceof VoltDBColumnReference) {
                    arg = kind == Kind.STRING
                            ? new VoltDBBinaryOperation(arg, new VoltDBConstant("''"), VoltDBBinaryOperator.CONCAT)
                            : new VoltDBBinaryOperation(arg, new VoltDBConstant("0"), VoltDBBinaryOperator.ADD);
                }
                args = List.of(arg);
            } finally {
                inAggregate = false;
            }
        }
        return VoltDBSubquery.scalar(table.getName(), alias, new VoltDBAggregate(args, function), where);
    }

    public List<VoltDBColumn> getColumns() {
        return columns;
    }

    // ENG-30510: VoltDB turns a RIGHT JOIN into a LEFT JOIN and can then use an ON condition on the preserved table
    // as its index range, dropping rows; unless --avoid-known-bugs is false, joins are INNER, LEFT or FULL (a RIGHT
    // JOIN is a LEFT JOIN with the tables swapped)
    private VoltDBJoin.JoinType randomJoinType() {
        if (avoidKnownBugs && noFullJoins) {
            return Randomly.fromOptions(VoltDBJoin.JoinType.INNER, VoltDBJoin.JoinType.LEFT);
        }
        if (avoidKnownBugs) {
            return Randomly.fromOptions(VoltDBJoin.JoinType.INNER, VoltDBJoin.JoinType.LEFT, VoltDBJoin.JoinType.FULL);
        }
        return VoltDBJoin.JoinType.getRandom();
    }

    // ENG-30531: GROUP BY or DISTINCT on an indexed column, inlined into a FULL JOIN as a serial aggregation, returns
    // the NULL group twice, as the null-extended rows come after the index-ordered ones. Unless --avoid-known-bugs is
    // false, a generator for such queries makes no FULL JOINs.
    public VoltDBExpressionGenerator setNoFullJoins(boolean noFullJoins) {
        this.noFullJoins = noFullJoins;
        return this;
    }

    // ENG-30533: MIN or MAX of a column without GROUP BY, with a WHERE clause that makes it the leading column of a
    // multi-column index's range, skips rows whose next index column is NULL. The shortcut applies to a bare column
    // only, so unless --avoid-known-bugs is false, a generator of aggregates without GROUP BY writes a numeric column c
    // as (c + 0) and a string column as (c || ''), and leaves out MIN and MAX of VARBINARY and temporal values, which
    // have no such identity.
    public VoltDBExpressionGenerator setUngroupedAggregates(boolean ungroupedAggregates) {
        this.ungroupedAggregates = ungroupedAggregates;
        return this;
    }

    private boolean avoidsMinMaxShortcut() {
        return avoidKnownBugs && ungroupedAggregates;
    }

    // ENG-12656: an index lookup on a VARBINARY(n) column fails for a longer value, so unless --avoid-known-bugs is
    // false, VARBINARY partitioning columns are joined only when they have the same size
    private boolean canPartitionJoin(VoltDBTable anchor, VoltDBTable t) {
        if (partitionKind(t) != partitionKind(anchor)) {
            return false;
        }
        return partitionKind(t) != Kind.VARBINARY || !avoidKnownBugs
                || partitionColumn(t).getType().getSize() == partitionColumn(anchor).getType().getSize();
    }

    private static Kind partitionKind(VoltDBTable t) {
        return Kind.fromType(t.getColumns().stream().filter(c -> c.getName().equals(t.getPartitionColumn())).findFirst()
                .orElseThrow(() -> new AssertionError(t.getName())).getType().getType());
    }

    // anchor.pc = t.pc and nothing else: VoltDB plans a join of partitioned tables only when it can see the equality
    // of their partitioning columns, and a further condition that folds to FALSE, such as pc = pc AND (1 = 0), takes
    // the equality with it ("This query is not plannable"). Random join conditions are left to replicated tables.
    private static VoltDBExpression partitionJoinCondition(VoltDBTable anchor, VoltDBTable t) {
        return new VoltDBBinaryOperation(new VoltDBColumnReference(partitionColumn(anchor)),
                new VoltDBColumnReference(partitionColumn(t)), VoltDBBinaryOperator.EQUALS);
    }

    private static VoltDBColumn partitionColumn(VoltDBTable t) {
        return t.getColumns().stream().filter(c -> c.getName().equals(t.getPartitionColumn())).findFirst()
                .orElseThrow(() -> new AssertionError(t.getName()));
    }

    // a random condition over the columns of the two tables
    private VoltDBExpression joinCondition(VoltDBTable anchor, VoltDBTable t, VoltDBJoin.JoinType type) {
        List<VoltDBColumn> joinColumns = new ArrayList<>(anchor.getColumns());
        // ENG-30510 (comment): a WHERE clause that rejects the NULLs of a FULL JOIN's right table makes it a RIGHT
        // JOIN, which VoltDB turns into a LEFT JOIN and then uses an ON condition on that table as its index range,
        // dropping rows (FROM a FULL JOIN b ON c >= b.k WHERE b.k IS NOT NULL); unless --avoid-known-bugs is false a
        // FULL JOIN's ON condition reads the left table only
        if (!(avoidKnownBugs && type == VoltDBJoin.JoinType.FULL)) {
            joinColumns.addAll(t.getColumns());
        }
        VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state).setColumns(joinColumns);
        // ENG-30497 applies to ON conditions too: a later join's condition sees the null-extended rows of the outer
        // joins before it, as (t0.b OR t2.b) IS NOT NULL in t1 LEFT JOIN t2 ... FULL JOIN t0 ON ... did
        gen.hasOuterJoin = true;
        // ENG-30614: an outer join whose ON condition folds to FALSE returns no rows at all, not the preserved rows
        // null-extended, so unless --avoid-known-bugs is false no AND, OR or NOT in an ON condition has a column-free
        // operand, and nothing in it folds to a constant
        for (int i = 0; i < 100; i++) {
            VoltDBExpression condition = gen.generatePredicateWithColumn();
            if (!avoidKnownBugs || !gen.hasConstantBooleanPart(condition)) {
                return condition;
            }
        }
        throw new IgnoreMeException();
    }

    // whether an operand of an AND, OR or NOT in the expression references no column
    private boolean hasConstantBooleanPart(VoltDBExpression expr) {
        if (expr instanceof VoltDBUnaryPrefixOperation && ((VoltDBUnaryPrefixOperation) expr)
                .getOperatorRepresentation().equals(VoltDBUnaryPrefixOperator.NOT.getTextRepresentation())) {
            VoltDBExpression operand = ((VoltDBUnaryPrefixOperation) expr).getExpr();
            return !referencesColumn(operand) || hasConstantBooleanPart(operand);
        }
        if (expr instanceof VoltDBBinaryOperation) {
            VoltDBBinaryOperation op = (VoltDBBinaryOperation) expr;
            String text = op.getOperatorRepresentation();
            if (text.equals(VoltDBBinaryOperator.AND.getTextRepresentation())
                    || text.equals(VoltDBBinaryOperator.OR.getTextRepresentation())) {
                return !referencesColumn(op.getLeft()) || !referencesColumn(op.getRight())
                        || hasConstantBooleanPart(op.getLeft()) || hasConstantBooleanPart(op.getRight());
            }
        }
        return false;
    }

    @Override
    public VoltDBExpression generatePredicate() {
        return generateExpression(Kind.BOOLEAN);
    }

    @Override
    public VoltDBExpression negatePredicate(VoltDBExpression predicate) {
        return not(predicate);
    }

    // ENG-30504: NOT (NOT (x OR y)) also fails when the OR is hidden behind a constant that the join planner folds
    // away, as in NOT (NOT ((1 = 1) AND (x OR y))), so unless --avoid-known-bugs is false NOT (NOT q) is written q,
    // which is the same in three-valued logic
    private VoltDBExpression not(VoltDBExpression expr) {
        if (avoidKnownBugs && expr instanceof VoltDBUnaryPrefixOperation && ((VoltDBUnaryPrefixOperation) expr)
                .getOperatorRepresentation().equals(VoltDBUnaryPrefixOperator.NOT.getTextRepresentation())) {
            return ((VoltDBUnaryPrefixOperation) expr).getExpr();
        }
        return new VoltDBUnaryPrefixOperation(expr, VoltDBUnaryPrefixOperator.NOT);
    }

    // ENG-30504: (NOT (K AND (x OR y))) IS NOT NULL fails too once the planner folds K away, so unless
    // --avoid-known-bugs is false, the operand of IS [NOT] NULL loses its leading NOTs; (NOT q) IS NULL is q IS NULL
    private VoltDBExpression withoutNot(VoltDBExpression expr) {
        VoltDBExpression e = expr;
        while (avoidKnownBugs && e instanceof VoltDBUnaryPrefixOperation && ((VoltDBUnaryPrefixOperation) e)
                .getOperatorRepresentation().equals(VoltDBUnaryPrefixOperator.NOT.getTextRepresentation())) {
            e = ((VoltDBUnaryPrefixOperation) e).getExpr();
        }
        return e;
    }

    // ENG-30516: written NOT (p IS NOT NULL) unless --avoid-known-bugs is false, as in generateIsNull
    @Override
    public VoltDBExpression isNull(VoltDBExpression expr) {
        if (avoidKnownBugs) {
            return new VoltDBUnaryPrefixOperation(
                    new VoltDBUnaryPostfixOperation(withoutNot(expr), VoltDBUnaryPostfixOperator.IS_NOT_NULL),
                    VoltDBUnaryPrefixOperator.NOT);
        }
        return new VoltDBUnaryPostfixOperation(expr, VoltDBUnaryPostfixOperator.IS_NULL);
    }

    // a random non-empty subset of the columns of the FROM clause, in random order
    public List<VoltDBExpression> generateFetchColumns() {
        return Randomly.nonEmptySubset(columns).stream().map(VoltDBColumnReference::new).collect(Collectors.toList());
    }

    // VoltDB rejects GROUP BY on a BOOLEAN expression, and so DISTINCT too ("A GROUP BY clause does not allow a
    // BOOLEAN expression"); grouping columns are a random non-empty subset of the others.
    // ENG-30530: grouping by the partitioning column of a table an outer join null-extends gives one NULL group per
    // partition, so unless --avoid-known-bugs is false, partitioning columns are not grouped by with an outer join
    public List<VoltDBExpression> generateGroupingColumns() {
        List<VoltDBColumn> groupable = columns.stream()
                .filter(c -> Kind.fromType(c.getType().getType()) != Kind.BOOLEAN)
                .filter(c -> !(avoidKnownBugs && hasOuterJoin && c.getName().equals(c.getTable().getPartitionColumn())))
                .collect(Collectors.toList());
        if (groupable.isEmpty()) {
            throw new IgnoreMeException();
        }
        List<VoltDBColumn> grouping = Randomly.nonEmptySubset(groupable);
        if (avoidKnownBugs) {
            // ENG-30529: a multi-partition GROUP BY or DISTINCT groups by only one of two columns of the same name,
            // such
            // as t0.c0 and t1.c0; aliases avoid it only for columns in the SELECT list, so the grouping columns have
            // distinct names
            List<String> names = new ArrayList<>();
            grouping = grouping.stream().filter(c -> {
                if (names.contains(c.getName())) {
                    return false;
                }
                names.add(c.getName());
                return true;
            }).collect(Collectors.toList());
        }
        return grouping.stream().map(VoltDBColumnReference::new).collect(Collectors.toList());
    }

    // 1 to 3 aggregates of random kinds
    public List<VoltDBExpression> generateAggregates() {
        List<VoltDBExpression> aggregates = new ArrayList<>();
        do {
            Kind kind;
            do {
                kind = getRandomType();
            } while (!canAggregate(kind)
                    || avoidsMinMaxShortcut() && (kind == Kind.VARBINARY || kind == Kind.TEMPORAL));
            aggregates.add(generateAggregate(kind));
        } while (aggregates.size() < 3 && Randomly.getBooleanWithRatherLowProbability());
        return aggregates;
    }

    // A predicate for HAVING, over the grouping columns and aggregates of the FROM clause's columns. VoltDB requires an
    // aggregate in a HAVING clause ("does not support HAVING clause without aggregation").
    public VoltDBExpression generateHavingPredicate(List<VoltDBExpression> groupingColumns) {
        List<VoltDBColumn> all = columns;
        columns = groupingColumns.stream().map(c -> ((VoltDBColumnReference) c).getColumn())
                .collect(Collectors.toList());
        aggregateColumns = all;
        try {
            for (int i = 0; i < 100; i++) {
                aggregated = false;
                VoltDBExpression predicate = generateExpression(Kind.BOOLEAN);
                if (aggregated) {
                    return predicate;
                }
            }
            throw new IgnoreMeException();
        } finally {
            columns = all;
            aggregateColumns = null;
        }
    }

    // ENG-30504: in a HAVING clause, NOT (a OR b) fails even as the whole clause ("Unable to determine cast type for
    // 'VoltType.BOOLEAN' and 'VoltType.BOOLEAN' types"), so unless --avoid-known-bugs is false, a HAVING predicate is
    // negated by pushing the NOT down to the operands of AND and OR (De Morgan's laws hold in three-valued logic)
    public VoltDBExpression negateHavingPredicate(VoltDBExpression predicate) {
        if (!avoidKnownBugs) {
            return negatePredicate(predicate);
        }
        if (predicate instanceof VoltDBBinaryOperation) {
            VoltDBBinaryOperation op = (VoltDBBinaryOperation) predicate;
            String text = op.getOperatorRepresentation();
            if (text.equals(VoltDBBinaryOperator.AND.getTextRepresentation())
                    || text.equals(VoltDBBinaryOperator.OR.getTextRepresentation())) {
                VoltDBBinaryOperator dual = text.equals(VoltDBBinaryOperator.AND.getTextRepresentation())
                        ? VoltDBBinaryOperator.OR : VoltDBBinaryOperator.AND;
                return new VoltDBBinaryOperation(negateHavingPredicate(op.getLeft()),
                        negateHavingPredicate(op.getRight()), dual);
            }
        }
        return not(predicate);
    }

    // ENG-30529 (see generateGroupingColumns): unless --avoid-known-bugs is false, every output column of a GROUP BY or
    // DISTINCT query is also given its own alias
    public List<VoltDBExpression> aliased(List<VoltDBExpression> fetchColumns) {
        if (!avoidKnownBugs) {
            return fetchColumns;
        }
        List<VoltDBExpression> result = new ArrayList<>();
        for (int i = 0; i < fetchColumns.size(); i++) {
            result.add(new VoltDBAlias(fetchColumns.get(i), "a" + i));
        }
        return result;
    }

    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    // The partitioned table that the other partitioned tables in FROM are joined to on their partitioning columns, so
    // that an equality on its partitioning column pins the query to one partition; null for replicated tables only.
    public VoltDBTable getPartitionAnchor() {
        return partitionAnchor;
    }

    // A WHERE predicate always references a column. VoltDB does not support a join filter that depends on none of the
    // joined tables, and its planner crashes on a WHERE clause that folds to a constant NULL (ENG-30495).
    @Override
    public VoltDBExpression generateBooleanExpression() {
        return generatePredicateWithColumn();
    }

    // whether the expression references a column in scope
    private boolean referencesColumn(VoltDBExpression expr) {
        String text = VoltDBToStringVisitor.asString(expr);
        return columns.stream().anyMatch(c -> text.contains(c.getFullQualifiedName()));
    }

    private VoltDBExpression generatePredicateWithColumn() {
        for (int i = 0; i < 100; i++) {
            VoltDBExpression predicate = generateExpression(Kind.BOOLEAN);
            if (referencesColumn(predicate)) {
                return predicate;
            }
        }
        throw new IgnoreMeException();
    }

    @Override
    public VoltDBSelect generateSelect() {
        return new VoltDBSelect();
    }

    @Override
    public List<VoltDBJoin> getRandomJoinClauses() {
        return joins;
    }

    @Override
    public List<VoltDBExpression> getTableRefs() {
        return fromTables.stream().map(VoltDBTableReference::new).collect(Collectors.toList());
    }

    @Override
    public String generateOptimizedQueryString(VoltDBSelect select, VoltDBExpression whereCondition,
            boolean shouldUseAggregate) {
        if (shouldUseAggregate) {
            select.setFetchColumns(List.of(new VoltDBConstant("COUNT(*)")));
        } else {
            select.setFetchColumns(columns.stream().map(VoltDBColumnReference::new).collect(Collectors.toList()));
            if (Randomly.getBooleanWithSmallProbability()) {
                select.setOrderByClauses(generateOrderBys());
            }
        }
        select.setWhereClause(whereCondition);
        return select.asString();
    }

    @Override
    public List<VoltDBExpression> generateOrderBys() {
        List<VoltDBColumn> orderable = columns.stream().filter(c -> canOrder(Kind.fromType(c.getType().getType())))
                .collect(Collectors.toList());
        if (orderable.isEmpty()) {
            return List.of();
        }
        return Randomly.nonEmptySubset(orderable).stream().map(VoltDBColumnReference::new).collect(Collectors.toList());
    }

    @Override
    public String generateUnoptimizedQueryString(VoltDBSelect select, VoltDBExpression whereCondition) {
        // The predicate moves from WHERE into the projection, where the planner cannot use it to filter. CASE counts
        // a row only when the predicate is TRUE, as WHERE does. COALESCE turns the SUM of no rows (NULL) into 0.
        String predicate = VoltDBToStringVisitor.asString(whereCondition);
        select.setFetchColumns(List.of(new VoltDBConstant("CASE WHEN " + predicate + " THEN 1 ELSE 0 END AS c")));
        select.setWhereClause(null);
        select.setOrderByClauses(List.of());
        if (predicate.contains("(SELECT ")) {
            // VoltDB rejects the SUM of an expression with a subquery, even through the derived table ("SQL Aggregate
            // function calls with subquery expression arguments are not allowed"); NoREC adds up the rows' values
            return select.asString();
        }
        return "SELECT COALESCE(SUM(c), 0) FROM (" + select.asString() + ") AS res";
    }

}
