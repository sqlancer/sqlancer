package sqlancer.tidb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.CERTGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBCompositeDataType;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.ast.TiDBAggregate;
import sqlancer.tidb.ast.TiDBAggregate.TiDBAggregateFunction;
import sqlancer.tidb.ast.TiDBBinaryBitOperation;
import sqlancer.tidb.ast.TiDBBinaryBitOperation.TiDBBinaryBitOperator;
import sqlancer.tidb.ast.TiDBBinaryComparisonOperation;
import sqlancer.tidb.ast.TiDBBinaryComparisonOperation.TiDBComparisonOperator;
import sqlancer.tidb.ast.TiDBBinaryLogicalOperation;
import sqlancer.tidb.ast.TiDBBinaryLogicalOperation.TiDBBinaryLogicalOperator;
import sqlancer.tidb.ast.TiDBCase;
import sqlancer.tidb.ast.TiDBCastOperation;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBFunctionCall;
import sqlancer.tidb.ast.TiDBFunctionCall.TiDBFunction;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBJoin.JoinType;
import sqlancer.tidb.ast.TiDBOrderingTerm;
import sqlancer.tidb.ast.TiDBRegexOperation;
import sqlancer.tidb.ast.TiDBRegexOperation.TiDBRegexOperator;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation.TiDBUnaryPrefixOperator;

public class TiDBExpressionGenerator extends UntypedExpressionGenerator<TiDBExpression, TiDBColumn>
        implements TLPWhereGenerator<TiDBSelect, TiDBJoin, TiDBExpression, TiDBTable, TiDBColumn>,
        CERTGenerator<TiDBSelect, TiDBJoin, TiDBExpression, TiDBTable, TiDBColumn> {

    private enum Gen {
        UNARY_PREFIX, //
        UNARY_POSTFIX, //
        CONSTANT, //
        COLUMN, //
        COMPARISON, REGEX, FUNCTION, BINARY_LOGICAL, BINARY_BIT, CAST, DEFAULT, CASE
        // BINARY_ARITHMETIC
    }

    private final TiDBGlobalState globalState;

    private List<TiDBTable> tables;

    public TiDBExpressionGenerator(TiDBGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public TiDBExpression generateConstant() {
        TiDBDataType type = TiDBDataType.getRandom();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return TiDBConstant.createNullConstant();
        }
        switch (type) {
        case INT:
            return TiDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case BLOB:
        case TEXT:
            return TiDBConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOL:
            return TiDBConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOATING:
            return TiDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        case CHAR:
            return TiDBConstant.createStringConstant(globalState.getRandomly().getChar());
        case DECIMAL:
        case NUMERIC:
            return TiDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public List<TiDBExpression> generateOrderBys() {
        List<TiDBExpression> expressions = super.generateOrderBys();
        List<TiDBExpression> newExpressions = new ArrayList<>();
        for (TiDBExpression expr : expressions) {
            TiDBExpression newExpr = expr;
            if (Randomly.getBoolean()) {
                newExpr = new TiDBOrderingTerm(expr, Randomly.getBoolean());
            }
            newExpressions.add(newExpr);
        }
        return newExpressions;
    }

    @Override
    public TiDBExpression negatePredicate(TiDBExpression predicate) {
        return new TiDBUnaryPrefixOperation(predicate, TiDBUnaryPrefixOperator.NOT);
    }

    @Override
    public TiDBExpression isNull(TiDBExpression expr) {
        return new TiDBUnaryPostfixOperation(expr, TiDBUnaryPostfixOperator.IS_NULL);
    }

    public TiDBExpression generateConstant(TiDBDataType type) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return TiDBConstant.createNullConstant();
        }
        switch (type) {
        case INT:
            return TiDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case BLOB:
        case TEXT:
            return TiDBConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOL:
            return TiDBConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOATING:
            return TiDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        case CHAR:
            return TiDBConstant.createStringConstant(globalState.getRandomly().getChar());
        case DECIMAL:
        case NUMERIC:
            return TiDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public TiDBExpressionGenerator setTablesAndColumns(AbstractTables<TiDBTable, TiDBColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public TiDBExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public TiDBSelect generateSelect() {
        return new TiDBSelect();
    }

    @Override
    public List<TiDBJoin> getRandomJoinClauses() {
        List<TiDBExpression> tableList = tables.stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        List<TiDBJoin> joins = TiDBJoin.getJoins(tableList, globalState);
        tables = tableList.stream().map(t -> ((TiDBTableReference) t).getTable()).collect(Collectors.toList());
        return joins;
    }

    @Override
    public List<TiDBExpression> getTableRefs() {
        return tables.stream().map(t -> new TiDBTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public List<TiDBExpression> generateFetchColumns(boolean shouldCreateDummy) {
        if (shouldCreateDummy && Randomly.getBoolean()) {
            return List.of(new TiDBColumnReference(
                    new TiDBColumn("*", new TiDBCompositeDataType(TiDBDataType.INT), false, false, false)));
        }
        return Randomly.nonEmptySubset(this.columns).stream().map(c -> new TiDBColumnReference(c))
                .collect(Collectors.toList());
    }

    @Override
    protected TiDBExpression generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            allowAggregates = false;
            TiDBAggregateFunction func = TiDBAggregateFunction.getRandom();
            List<TiDBExpression> args = generateExpressions(func.getNrArgs());
            return new TiDBAggregate(args, func);
        }
        switch (Randomly.fromOptions(Gen.values())) {
        case DEFAULT:
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
            TiDBColumn column = Randomly.fromList(columns);
            if (column.hasDefault()) {
                return new TiDBFunctionCall(TiDBFunction.DEFAULT, Arrays.asList(new TiDBColumnReference(column)));
            }
            throw new IgnoreMeException();
        case UNARY_POSTFIX:
            return new TiDBUnaryPostfixOperation(generateExpression(depth + 1), TiDBUnaryPostfixOperator.getRandom());
        case UNARY_PREFIX:
            TiDBUnaryPrefixOperator rand = TiDBUnaryPrefixOperator.getRandom();
            return new TiDBUnaryPrefixOperation(generateExpression(depth + 1), rand);
        case COLUMN:
            return generateColumn();
        case CONSTANT:
            return generateConstant();
        case COMPARISON:
            return new TiDBBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    TiDBComparisonOperator.getRandom());
        case REGEX:
            return new TiDBRegexOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    TiDBRegexOperator.getRandom());
        case FUNCTION:
            TiDBFunction func = TiDBFunction.getRandom();
            return new TiDBFunctionCall(func, generateExpressions(func.getNrArgs(), depth));
        case BINARY_BIT:
            return new TiDBBinaryBitOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    TiDBBinaryBitOperator.getRandom());
        case BINARY_LOGICAL:
            return new TiDBBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    TiDBBinaryLogicalOperator.getRandom());
        case CAST:
            return new TiDBCastOperation(generateExpression(depth + 1), Randomly.fromOptions("BINARY", // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/52
                    "CHAR", "DATE", "DATETIME", "TIME", // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/13
                    "DECIMAL", "SIGNED", "UNSIGNED" /* https://github.com/pingcap/tidb/issues/16028 */));
        case CASE:
            int nr = Randomly.fromOptions(1, 2);
            return new TiDBCase(generateExpression(depth + 1), generateExpressions(nr, depth + 1),
                    generateExpressions(nr, depth + 1), generateExpression(depth + 1));
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected TiDBExpression generateColumn() {
        TiDBColumn column = Randomly.fromList(columns);
        return new TiDBColumnReference(column);
    }

    @Override
    public String generateExplainQuery(TiDBSelect select) {
        return "EXPLAIN " + select.asString();
    }

    @Override
    public boolean mutate(TiDBSelect select) {
        List<Function<TiDBSelect, Boolean>> mutators = new ArrayList<>();

        mutators.add(this::mutateJoin);
        mutators.add(this::mutateWhere);
        if (!TiDBBugs.bug38319) {
            mutators.add(this::mutateGroupBy);
            mutators.add(this::mutateHaving);
        }
        mutators.add(this::mutateAnd);
        if (!TiDBBugs.bug51525) {
            mutators.add(this::mutateOr);
        }
        mutators.add(this::mutateLimit);
        // mutators.add(this::mutateDistinct);

        return Randomly.fromList(mutators).apply(select);
    }

    boolean mutateJoin(TiDBSelect select) {
        if (select.getJoinList().isEmpty()) {
            return false;
        }
        TiDBJoin join = (TiDBJoin) Randomly.fromList(select.getJoinList());
        if (join.getJoinType() == JoinType.NATURAL) {
            return false;
        }

        // CROSS does not need ON Condition, while other joins do
        // To avoid Null pointer, generating a new new condition when mutating CROSS to
        // other joins
        if (join.getJoinType() == JoinType.CROSS) {
            List<TiDBColumn> columns = new ArrayList<>();
            columns.addAll(((TiDBTableReference) join.getLeftTable()).getTable().getColumns());
            columns.addAll(((TiDBTableReference) join.getRightTable()).getTable().getColumns());
            TiDBExpressionGenerator joinGen2 = new TiDBExpressionGenerator(globalState).setColumns(columns);
            join.setOnCondition(joinGen2.generateExpression());
        }

        JoinType newJoinType = TiDBJoin.JoinType.INNER;
        if (join.getJoinType() == JoinType.LEFT || join.getJoinType() == JoinType.RIGHT) { // No invarient relation
                                                                                           // between LEFT and RIGHT
                                                                                           // join
            newJoinType = JoinType.getRandomExcept(JoinType.NATURAL, JoinType.LEFT, JoinType.RIGHT);
        } else {
            newJoinType = JoinType.getRandomExcept(JoinType.NATURAL, join.getJoinType());
        }
        assert newJoinType != JoinType.NATURAL; // Natural Join is not supported for CERT
        boolean increase = join.getJoinType().ordinal() < newJoinType.ordinal();
        join.setJoinType(newJoinType);
        if (newJoinType == JoinType.CROSS) {
            join.setOnCondition(null);
        }
        return increase;
    }

    boolean mutateWhere(TiDBSelect select) {
        boolean increase = select.getWhereClause() != null;
        if (increase) {
            select.setWhereClause(null);
        } else {
            select.setWhereClause(generateExpression());
        }
        return increase;
    }

    boolean mutateHaving(TiDBSelect select) {
        if (select.getGroupByExpressions().size() == 0) {
            select.setGroupByExpressions(select.getFetchColumns());
            select.setHavingClause(generateExpression());
            return false;
        } else {
            if (select.getHavingClause() == null) {
                select.setHavingClause(generateExpression());
                return false;
            } else {
                select.setHavingClause(null);
                return true;
            }
        }
    }

    boolean mutateAnd(TiDBSelect select) {
        if (select.getWhereClause() == null) {
            select.setWhereClause(generateExpression());
        } else {
            TiDBExpression newWhere = new TiDBBinaryLogicalOperation(select.getWhereClause(), generateExpression(),
                    TiDBBinaryLogicalOperator.AND);
            select.setWhereClause(newWhere);
        }
        return false;
    }

    boolean mutateOr(TiDBSelect select) {
        if (select.getWhereClause() == null) {
            select.setWhereClause(generateExpression());
            return false;
        } else {
            TiDBExpression newWhere = new TiDBBinaryLogicalOperation(select.getWhereClause(), generateExpression(),
                    TiDBBinaryLogicalOperator.OR);
            select.setWhereClause(newWhere);
            return true;
        }
    }

    boolean mutateLimit(TiDBSelect select) {
        boolean increase = select.getLimitClause() != null;
        if (increase) {
            select.setLimitClause(null);
        } else {
            select.setLimitClause(generateConstant(TiDBDataType.INT));
        }
        return increase;
    }

    private boolean mutateGroupBy(TiDBSelect select) {
        boolean increase = select.getGroupByExpressions().size() > 0;
        if (increase) {
            select.clearGroupByExpressions();
            select.clearHavingClause();
        } else {
            select.setGroupByExpressions(select.getFetchColumns());
        }
        return increase;
    }
}
