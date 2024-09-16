package sqlancer.datafusion.gen;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;
import static sqlancer.datafusion.gen.DataFusionBaseExprFactory.createExpr;
import static sqlancer.datafusion.gen.DataFusionBaseExprFactory.getExprsWithReturnType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionDataType;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.ast.DataFusionBinaryOperation;
import sqlancer.datafusion.ast.DataFusionColumnReference;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionFunction;
import sqlancer.datafusion.ast.DataFusionJoin;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.ast.DataFusionTableReference;
import sqlancer.datafusion.ast.DataFusionUnaryPostfixOperation;
import sqlancer.datafusion.ast.DataFusionUnaryPrefixOperation;
import sqlancer.datafusion.gen.DataFusionBaseExpr.ArgumentType;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprType;

public final class DataFusionExpressionGenerator
        extends TypedExpressionGenerator<DataFusionExpression, DataFusionColumn, DataFusionDataType> implements
        NoRECGenerator<DataFusionSelect, DataFusionJoin, DataFusionExpression, DataFusionTable, DataFusionColumn>,
        TLPWhereGenerator<DataFusionSelect, DataFusionJoin, DataFusionExpression, DataFusionTable, DataFusionColumn> {

    private List<DataFusionTable> tables;
    private final DataFusionGlobalState globalState;

    public DataFusionExpressionGenerator(DataFusionGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    protected DataFusionDataType getRandomType() {
        DataFusionDataType dt;
        do {
            dt = Randomly.fromOptions(DataFusionDataType.values());
        } while (dt == DataFusionDataType.NULL);

        return dt;
    }

    @Override
    protected boolean canGenerateColumnOfType(DataFusionDataType type) {
        return true;
    }

    @Override
    protected DataFusionExpression generateExpression(DataFusionDataType type, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            DataFusionDataType expectedType = type;
            if (Randomly.getBooleanWithRatherLowProbability()) { // ~10%
                expectedType = DataFusionDataType.getRandomWithoutNull();
            }
            return generateLeafNode(expectedType);
        }

        // nested aggregate is not allowed, so occasionally apply it
        Boolean includeAggr = Randomly.getBooleanWithSmallProbability();
        List<DataFusionBaseExpr> possibleBaseExprs = getExprsWithReturnType(Optional.of(type)).stream()
                // Conditinally apply filter if `includeAggr` set to false
                .filter(expr -> includeAggr || expr.exprType != DataFusionBaseExpr.DataFusionBaseExprCategory.AGGREGATE)
                .collect(Collectors.toList());

        if (possibleBaseExprs.isEmpty()) {
            dfAssert(type == DataFusionDataType.NULL, "should able to generate expression with type " + type);
            return generateLeafNode(type);
        }

        DataFusionBaseExpr randomExpr = Randomly.fromList(possibleBaseExprs);
        switch (randomExpr.exprType) {
        case UNARY_PREFIX:
            DataFusionDataType argType = null;
            dfAssert(randomExpr.argTypes.size() == 1 && randomExpr.nArgs == 1,
                    "Unary expression should only have 1 argument" + randomExpr.argTypes);
            if (randomExpr.argTypes.get(0) instanceof ArgumentType.Fixed) {
                ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) randomExpr.argTypes.get(0);
                argType = Randomly.fromList(possibleArgTypes.fixedType);
            } else {
                argType = type;
            }

            return new DataFusionUnaryPrefixOperation(generateExpression(argType, depth + 1), randomExpr);
        case UNARY_POSTFIX:
            dfAssert(randomExpr.argTypes.size() == 1 && randomExpr.nArgs == 1,
                    "Unary expression should only have 1 argument" + randomExpr.argTypes);
            if (randomExpr.argTypes.get(0) instanceof ArgumentType.Fixed) {
                ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) randomExpr.argTypes.get(0);
                argType = Randomly.fromList(possibleArgTypes.fixedType);
            } else {
                argType = type;
            }

            return new DataFusionUnaryPostfixOperation(generateExpression(argType, depth + 1), randomExpr);
        case BINARY:
            dfAssert(randomExpr.argTypes.size() == 2 && randomExpr.nArgs == 2,
                    "Binrary expression should only have 2 argument" + randomExpr.argTypes);
            List<DataFusionDataType> argTypeList = new ArrayList<>(); // types of current expression's input
                                                                      // arguments
            for (ArgumentType argumentType : randomExpr.argTypes) {
                if (argumentType instanceof ArgumentType.Fixed) {
                    ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) randomExpr.argTypes.get(0);
                    dfAssert(!possibleArgTypes.fixedType.isEmpty(), "possible types can't be an empty list");
                    DataFusionDataType determinedType = Randomly.fromList(possibleArgTypes.fixedType);
                    argTypeList.add(determinedType);
                } else if (argumentType instanceof ArgumentType.SameAsFirstArgType) {
                    dfAssert(!argTypeList.isEmpty(), "First argument can't have argument type `SameAsFirstArgType`");
                    DataFusionDataType firstArgType = argTypeList.get(0);
                    argTypeList.add(firstArgType);
                } else {
                    // Same as expression return type
                    argTypeList.add(type);
                }
            }

            return new DataFusionBinaryOperation(generateExpression(argTypeList.get(0), depth + 1),
                    generateExpression(argTypeList.get(1), depth + 1), randomExpr);
        case AGGREGATE:
            // Fall through
        case FUNC:
            return generateFunctionExpression(type, depth, randomExpr);
        default:
            dfAssert(false, "unreachable");
        }

        dfAssert(false, "unreachable");
        return null;
    }

    public DataFusionExpression generateFunctionExpression(DataFusionDataType type, int depth,
            DataFusionBaseExpr exprType) {
        if (exprType.isVariadic || Randomly.getBooleanWithSmallProbability()) {
            // TODO(datafusion) maybe add possible types. e.g. some function have signature
            // variadic(INT/DOUBLE), then
            // only randomly pick from INT and DOUBLE
            int nArgs = Randomly.smallNumber(); // 0, 2, 4, ... smaller one is more likely
            return new DataFusionFunction<DataFusionBaseExpr>(generateExpressions(nArgs), exprType);
        }

        List<DataFusionDataType> funcArgTypeList = new ArrayList<>(); // types of current expression's input arguments
        int i = 0;
        for (ArgumentType argumentType : exprType.argTypes) {
            if (argumentType instanceof ArgumentType.Fixed) {
                ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) exprType.argTypes.get(i);
                dfAssert(!possibleArgTypes.fixedType.isEmpty(), "possible types can't be an empty list");
                DataFusionDataType determinedType = Randomly.fromList(possibleArgTypes.fixedType);
                funcArgTypeList.add(determinedType);
            } else if (argumentType instanceof ArgumentType.SameAsFirstArgType) {
                dfAssert(!funcArgTypeList.isEmpty(), "First argument can't have argument type `SameAsFirstArgType`");
                DataFusionDataType firstArgType = funcArgTypeList.get(0);
                funcArgTypeList.add(firstArgType);
            } else {
                // Same as expression return type
                funcArgTypeList.add(type);
            }
            i++;
        }

        List<DataFusionExpression> argExpressions = new ArrayList<>();

        for (DataFusionDataType dataType : funcArgTypeList) {
            argExpressions.add(generateExpression(dataType, depth + 1));
        }

        return new DataFusionFunction<DataFusionBaseExpr>(argExpressions, exprType);
    }

    List<DataFusionColumn> filterColumns(DataFusionDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    @Override
    protected DataFusionExpression generateColumn(DataFusionDataType type) {
        // HACK: if no col of such type exist, generate constant value instead
        List<DataFusionColumn> colsOfType = filterColumns(type);
        if (colsOfType.isEmpty()) {
            return generateConstant(type);
        }

        DataFusionColumn column = Randomly.fromList(colsOfType);
        return new DataFusionColumnReference(column);
    }

    @Override
    public DataFusionExpression generateConstant(DataFusionDataType type) {
        return type.getRandomConstant(globalState);
    }

    @Override
    public DataFusionExpression generatePredicate() {
        return generateExpression(DataFusionDataType.BOOLEAN, 0);
    }

    @Override
    public DataFusionExpression negatePredicate(DataFusionExpression predicate) {
        return new DataFusionUnaryPrefixOperation(predicate, createExpr(DataFusionBaseExprType.NOT));
    }

    @Override
    public DataFusionExpression isNull(DataFusionExpression expr) {
        return new DataFusionUnaryPostfixOperation(expr, createExpr(DataFusionBaseExprType.IS_NULL));
    }

    public static class DataFusionCastOperation extends NewUnaryPostfixOperatorNode<DataFusionExpression> {

        public DataFusionCastOperation(DataFusionExpression expr, DataFusionDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    @Override
    public DataFusionExpressionGenerator setTablesAndColumns(AbstractTables<DataFusionTable, DataFusionColumn> tables) {
        List<DataFusionTable> randomTables = Randomly.nonEmptySubset(tables.getTables());
        int maxSize = Randomly.fromOptions(1, 2, 3, 4);
        if (randomTables.size() > maxSize) {
            randomTables = randomTables.subList(0, maxSize);
        }
        this.columns = DataFusionTable.getAllColumns(randomTables);
        this.tables = randomTables;

        return this;
    }

    @Override
    public DataFusionExpression generateBooleanExpression() {
        return generateExpression(DataFusionDataType.BOOLEAN);
    }

    @Override
    public DataFusionSelect generateSelect() {
        return new DataFusionSelect();
    }

    @Override
    public List<DataFusionJoin> getRandomJoinClauses() {
        List<DataFusionTableReference> tableList = tables.stream().map(t -> new DataFusionTableReference(t))
                .collect(Collectors.toList());
        List<DataFusionJoin> joins = DataFusionJoin.getJoins(tableList, globalState);
        tables = tableList.stream().map(t -> t.getTable()).collect(Collectors.toList());
        return joins;
    }

    @Override
    public List<DataFusionExpression> getTableRefs() {
        return tables.stream().map(t -> new DataFusionTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public String generateOptimizedQueryString(DataFusionSelect select, DataFusionExpression whereCondition,
            boolean shouldUseAggregate) {
        if (shouldUseAggregate) {
            select.setFetchColumnsString("COUNT(*)");
        } else {
            List<DataFusionExpression> allColumns = columns.stream().map((c) -> new DataFusionColumnReference(c))
                    .collect(Collectors.toList());
            select.setFetchColumns(allColumns);
        }
        select.setWhereClause(whereCondition);

        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(DataFusionSelect select, DataFusionExpression whereCondition) {
        String fetchColumn = String.format("COUNT(CASE WHEN %S THEN 1 ELSE NULL END)",
                DataFusionToStringVisitor.asString(whereCondition));
        select.setFetchColumnsString(fetchColumn);
        select.setWhereClause(null);

        return select.asString();
    }

    @Override
    public List<DataFusionExpression> generateFetchColumns(boolean shouldCreateDummy) {
        List<DataFusionColumn> randomColumns = DataFusionTable.getRandomColumns(tables);
        return randomColumns.stream().map((c) -> new DataFusionColumnReference(c)).collect(Collectors.toList());
    }
}
