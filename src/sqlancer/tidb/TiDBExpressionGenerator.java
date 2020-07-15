package sqlancer.tidb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.gen.UntypedExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
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
import sqlancer.tidb.ast.TiDBCollate;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBFunctionCall;
import sqlancer.tidb.ast.TiDBFunctionCall.TiDBFunction;
import sqlancer.tidb.ast.TiDBOrderingTerm;
import sqlancer.tidb.ast.TiDBRegexOperation;
import sqlancer.tidb.ast.TiDBRegexOperation.TiDBRegexOperator;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation.TiDBUnaryPrefixOperator;

public class TiDBExpressionGenerator extends UntypedExpressionGenerator<TiDBExpression, TiDBColumn> {

    private final TiDBGlobalState globalState;

    public TiDBExpressionGenerator(TiDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Gen {
        UNARY_PREFIX, //
        UNARY_POSTFIX, //
        CONSTANT, //
        COLUMN, //
        COMPARISON, REGEX, COLLATE, FUNCTION, BINARY_LOGICAL, BINARY_BIT, CAST, DEFAULT, CASE
        // BINARY_ARITHMETIC
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
            if (TiDBBugs.bug15) {
                throw new IgnoreMeException();
            }
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
            return new TiDBFunctionCall(TiDBFunction.DEFAULT, Arrays.asList(generateColumn()));
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
        case COLLATE:
            return new TiDBCollate(generateExpression(depth + 1),
                    Randomly.fromOptions("utf8mb4_bin", "latin1_bin", "binary", "ascii_bin", "utf8_bin"));
        case FUNCTION:
            TiDBFunction func = TiDBFunction.getRandom();
            return new TiDBFunctionCall(func, generateExpressions(depth, func.getNrArgs()));
        case BINARY_BIT:
            return new TiDBBinaryBitOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    TiDBBinaryBitOperator.getRandom());
        case BINARY_LOGICAL:
            if (TiDBBugs.bug48) {
                throw new IgnoreMeException();
            }
            return new TiDBBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    TiDBBinaryLogicalOperator.getRandom());
        // case BINARY_ARITHMETIC:
        // return new TiDBBinaryArithmeticOperation(generateExpression(depth + 1), generateExpression(depth + 1),
        // TiDBBinaryArithmeticOperator.getRandom());
        case CAST:
            return new TiDBCastOperation(generateExpression(depth + 1), Randomly.fromOptions(
                    /*
                     * "BINARY" https://github.com/tidb-challenge-program/bug-hunting-issue/issues/52
                     */ "CHAR",
                    /*
                     * "DATE", "DATETIME", "TIME", https://github.com/tidb-challenge-program/bug-hunting-issue/issues/13
                     */ "DECIMAL", "SIGNED"/* , "UNSIGNED" https://github.com/pingcap/tidb/issues/16028 */));
        case CASE:
            if (TiDBBugs.bug19) {
                throw new IgnoreMeException();
            }
            int nr = Randomly.fromOptions(1, 2);
            return new TiDBCase(generateExpression(depth + 1), generateExpressions(depth + 1, nr),
                    generateExpressions(depth + 1, nr), generateExpression(depth + 1));
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

}
