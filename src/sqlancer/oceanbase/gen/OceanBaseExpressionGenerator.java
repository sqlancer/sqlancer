package sqlancer.oceanbase.gen;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseRowValue;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.ast.OceanBaseAggregate;
import sqlancer.oceanbase.ast.OceanBaseAggregate.OceanBaseAggregateFunction;
import sqlancer.oceanbase.ast.OceanBaseBinaryComparisonOperation;
import sqlancer.oceanbase.ast.OceanBaseBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.oceanbase.ast.OceanBaseBinaryLogicalOperation;
import sqlancer.oceanbase.ast.OceanBaseBinaryLogicalOperation.OceanBaseBinaryLogicalOperator;
import sqlancer.oceanbase.ast.OceanBaseCastOperation;
import sqlancer.oceanbase.ast.OceanBaseColumnReference;
import sqlancer.oceanbase.ast.OceanBaseComputableFunction;
import sqlancer.oceanbase.ast.OceanBaseComputableFunction.OceanBaseFunction;
import sqlancer.oceanbase.ast.OceanBaseConstant;
import sqlancer.oceanbase.ast.OceanBaseConstant.OceanBaseDoubleConstant;
import sqlancer.oceanbase.ast.OceanBaseExists;
import sqlancer.oceanbase.ast.OceanBaseExpression;
import sqlancer.oceanbase.ast.OceanBaseInOperation;
import sqlancer.oceanbase.ast.OceanBaseJoin;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.ast.OceanBaseStringExpression;
import sqlancer.oceanbase.ast.OceanBaseTableReference;
import sqlancer.oceanbase.ast.OceanBaseText;
import sqlancer.oceanbase.ast.OceanBaseUnaryPostfixOperation;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator;

public class OceanBaseExpressionGenerator extends UntypedExpressionGenerator<OceanBaseExpression, OceanBaseColumn>
        implements
        NoRECGenerator<OceanBaseSelect, OceanBaseJoin, OceanBaseExpression, OceanBaseTable, OceanBaseColumn> {

    private OceanBaseGlobalState state;
    private OceanBaseRowValue rowVal;
    private List<OceanBaseTable> tables;

    public OceanBaseExpressionGenerator(OceanBaseGlobalState state) {
        this.state = state;
    }

    public OceanBaseExpressionGenerator setCon(Connection con) {
        return this;
    }

    public OceanBaseExpressionGenerator setState(OceanBaseGlobalState state) {
        this.state = state;
        return this;
    }

    public OceanBaseExpressionGenerator setOceanBaseColumns(List<OceanBaseSchema.OceanBaseColumn> columns) {
        return this;
    }

    public OceanBaseExpressionGenerator setRowVal(OceanBaseRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION, BINARY_LOGICAL_OPERATOR,
        BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, EXISTS;
    }

    @Override
    public OceanBaseExpression generateExpression(int depth) {
        if (depth >= state.getOptions().getMaxExpressionDepth()) {
            return generateLeafNode();
        }
        switch (Randomly.fromOptions(Actions.values())) {
        case COLUMN:
            return generateColumn();
        case LITERAL:
            return generateConstant();
        case UNARY_PREFIX_OPERATION:
            OceanBaseExpression subExpr = generateExpression(depth + 1);
            OceanBaseUnaryPrefixOperator random = OceanBaseUnaryPrefixOperator.getRandom();
            return new OceanBaseUnaryPrefixOperation(subExpr, random);
        case UNARY_POSTFIX:
            return new OceanBaseUnaryPostfixOperation(generateExpression(depth + 1),
                    Randomly.fromOptions(OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.values()),
                    Randomly.getBoolean());
        case COMPUTABLE_FUNCTION:
            return getComputableFunction(depth + 1);
        case BINARY_LOGICAL_OPERATOR:
            return new OceanBaseBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    OceanBaseBinaryLogicalOperator.getRandom());
        case BINARY_COMPARISON_OPERATION:
            return new OceanBaseBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    BinaryComparisonOperator.getRandom());
        case CAST:
            return new OceanBaseCastOperation(generateExpression(depth + 1),
                    OceanBaseCastOperation.CastType.getRandom());
        case IN_OPERATION:
            OceanBaseExpression expr = generateExpression(depth + 1);
            List<OceanBaseExpression> rightList = new ArrayList<>();
            for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                rightList.add(generateExpression(depth + 1));
            }
            return new OceanBaseInOperation(expr, rightList, Randomly.getBoolean());
        case EXISTS:
            return getExists();
        default:
            throw new AssertionError();
        }
    }

    private OceanBaseExpression getExists() {
        if (Randomly.getBoolean()) {
            return new OceanBaseExists(new OceanBaseStringExpression("SELECT 1", OceanBaseConstant.createTrue()));
        } else {
            return new OceanBaseExists(
                    new OceanBaseStringExpression("SELECT 1 from dual wHERE FALSE", OceanBaseConstant.createFalse()));
        }
    }

    private OceanBaseExpression getComputableFunction(int depth) {
        OceanBaseFunction func = OceanBaseFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        OceanBaseExpression[] args = new OceanBaseExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = generateExpression(depth + 1);
        }
        return new OceanBaseComputableFunction(func, args);
    }

    private enum ConstantType {
        INT, NULL, STRING, DOUBLE;

        public static ConstantType[] valuesPQS() {
            return new ConstantType[] { INT, NULL, STRING };
        }
    }

    @Override
    public OceanBaseExpression generateConstant() {
        ConstantType[] values;
        if (state.usesPQS()) {
            values = ConstantType.valuesPQS();
        } else {
            values = ConstantType.values();
        }
        OceanBaseConstant constant;
        switch (Randomly.fromOptions(values)) {
        case INT:
            return OceanBaseConstant.createIntConstant((int) state.getRandomly().getInteger());
        case NULL:
            return OceanBaseConstant.createNullConstant();
        case STRING:
            String string = state.getRandomly().getString().replace("\\", "").replace("\n", "").replace("\t", "");
            constant = OceanBaseConstant.createStringConstant(string);
            return constant;
        case DOUBLE:
            double val = state.getRandomly().getDouble();
            constant = new OceanBaseDoubleConstant(val);
            return constant;
        default:
            throw new AssertionError();
        }
    }

    @Override
    public OceanBaseExpression generateColumn() {
        OceanBaseColumn c = Randomly.fromList(columns);
        OceanBaseConstant val;
        if (rowVal == null) {
            val = OceanBaseConstant.createNullConstant();
        } else {
            val = rowVal.getValues().get(c);
        }
        return OceanBaseColumnReference.create(c, val);
    }

    public OceanBaseExpression generateConstant(OceanBaseColumn col) {
        OceanBaseConstant constant;
        switch (col.getType().name()) {
        case "INT":
            return OceanBaseConstant.createIntConstant((int) state.getRandomly().getInteger());
        case "NULL":
            return OceanBaseConstant.createNullConstant();
        case "VARCHAR":
            String string = state.getRandomly().getString().replace("\\", "").replace("\n", "").replace("\t", "");
            constant = OceanBaseConstant.createStringConstant(string);
            return constant;
        case "DOUBLE":
            double val = state.getRandomly().getDouble();
            constant = new OceanBaseDoubleConstant(val);
            return constant;
        case "FLOAT":
            val = state.getRandomly().getDouble();
            constant = new OceanBaseDoubleConstant(val);
            return constant;
        case "DECIMAL":
            val = state.getRandomly().getDouble();
            return new OceanBaseDoubleConstant(val);
        default:
            throw new AssertionError();
        }
    }

    @Override
    public OceanBaseExpression negatePredicate(OceanBaseExpression predicate) {
        return new OceanBaseUnaryPrefixOperation(predicate, OceanBaseUnaryPrefixOperator.NOT);
    }

    @Override
    public OceanBaseExpression isNull(OceanBaseExpression expr) {
        return new OceanBaseUnaryPostfixOperation(expr, OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL,
                false);
    }

    @Override
    public NoRECGenerator<OceanBaseSelect, OceanBaseJoin, OceanBaseExpression, OceanBaseTable, OceanBaseColumn> setTablesAndColumns(
            AbstractTables<OceanBaseTable, OceanBaseColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public OceanBaseExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public OceanBaseSelect generateSelect() {
        return new OceanBaseSelect();
    }

    @Override
    public List<OceanBaseJoin> getRandomJoinClauses() {
        return List.of();
    }

    @Override
    public List<OceanBaseExpression> getTableRefs() {
        return tables.stream().map(t -> new OceanBaseTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public String generateOptimizedQueryString(OceanBaseSelect select, OceanBaseExpression whereCondition,
            boolean shouldUseAggregate) {
        if (shouldUseAggregate) {
            OceanBaseExpression aggr = new OceanBaseAggregate(
                    new OceanBaseColumnReference(new OceanBaseColumn("*", OceanBaseDataType.INT, false, 0, false),
                            null),
                    OceanBaseAggregateFunction.COUNT);
            select.setFetchColumns(List.of(aggr));
        } else {
            List<OceanBaseExpression> allColumns = columns.stream().map((c) -> new OceanBaseColumnReference(c, null))
                    .collect(Collectors.toList());
            select.setFetchColumns(allColumns);
            if (Randomly.getBooleanWithSmallProbability()) {
                select.setOrderByClauses(generateOrderBys());
            }
        }
        select.setWhereClause(whereCondition);

        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(OceanBaseSelect select, OceanBaseExpression whereCondition) {
        OceanBaseExpression expr = getTrueExpr(whereCondition);

        OceanBaseText asText = new OceanBaseText(expr, " as count", false);
        select.setFetchColumns(List.of(asText));
        select.setSelectType(OceanBaseSelect.SelectType.ALL);

        return "SELECT SUM(count) FROM (" + select.asString() + ") as asdf";
    }

    private enum Option {
        TRUE, FALSE_NULL, NOT_NOT_TRUE, NOT_FALSE_NOT_NULL, IF, IFNULL, COALESCE
    };

    private OceanBaseExpression getTrueExpr(OceanBaseExpression randomWhereCondition) {
        // we can treat "is true" as combinations of "is flase" and "not","is not true" and "not",etc.
        OceanBaseUnaryPostfixOperation isTrue = new OceanBaseUnaryPostfixOperation(randomWhereCondition,
                OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_TRUE, false);

        OceanBaseUnaryPostfixOperation isFalse = new OceanBaseUnaryPostfixOperation(randomWhereCondition,
                OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_FALSE, false);

        OceanBaseUnaryPostfixOperation isNotFalse = new OceanBaseUnaryPostfixOperation(randomWhereCondition,
                OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_FALSE, true);

        OceanBaseUnaryPostfixOperation isNULL = new OceanBaseUnaryPostfixOperation(randomWhereCondition,
                OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);

        OceanBaseUnaryPostfixOperation isNotNULL = new OceanBaseUnaryPostfixOperation(randomWhereCondition,
                OceanBaseUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, true);

        OceanBaseExpression expr = OceanBaseConstant.createNullConstant();
        Option a = Randomly.fromOptions(Option.values());
        switch (a) {
        case TRUE:
            expr = isTrue;
            break;
        case FALSE_NULL:
            // not((is false) or (is null))
            expr = new OceanBaseUnaryPrefixOperation(
                    new OceanBaseBinaryLogicalOperation(isFalse, isNULL,
                            OceanBaseBinaryLogicalOperation.OceanBaseBinaryLogicalOperator.OR),
                    OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator.NOT);
            break;
        case NOT_NOT_TRUE:
            // not(not(is true)))
            expr = new OceanBaseUnaryPrefixOperation(
                    new OceanBaseUnaryPrefixOperation(isTrue,
                            OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator.NOT),
                    OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator.NOT);
            break;
        case NOT_FALSE_NOT_NULL:
            // (is not false) and (is not null)
            expr = new OceanBaseBinaryLogicalOperation(isNotFalse, isNotNULL,
                    OceanBaseBinaryLogicalOperation.OceanBaseBinaryLogicalOperator.AND);
            break;
        case IF:
            // if(1, xx is true, 0)
            OceanBaseExpression[] args = new OceanBaseExpression[3];
            args[0] = OceanBaseConstant.createIntConstant(1);
            args[1] = isTrue;
            args[2] = OceanBaseConstant.createIntConstant(0);
            expr = new OceanBaseComputableFunction(OceanBaseFunction.IF, args);
            break;
        case IFNULL:
            // ifnull(null, xx is true)
            OceanBaseExpression[] ifArgs = new OceanBaseExpression[2];
            ifArgs[0] = OceanBaseConstant.createNullConstant();
            ifArgs[1] = isTrue;
            expr = new OceanBaseComputableFunction(OceanBaseFunction.IFNULL, ifArgs);
            break;
        case COALESCE:
            // coalesce(null, xx is true)
            OceanBaseExpression[] coalesceArgs = new OceanBaseExpression[2];
            coalesceArgs[0] = OceanBaseConstant.createNullConstant();
            coalesceArgs[1] = isTrue;
            expr = new OceanBaseComputableFunction(OceanBaseFunction.COALESCE, coalesceArgs);
            break;
        default:
            expr = isTrue;
            break;
        }
        return expr;
    }
}
