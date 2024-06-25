package sqlancer.cnosdb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBRowValue;
import sqlancer.cnosdb.ast.CnosDBColumnValue;
import sqlancer.cnosdb.ast.CnosDBConstant;
import sqlancer.cnosdb.ast.CnosDBExpression;
import sqlancer.cnosdb.ast.CnosDBFunction;
import sqlancer.cnosdb.ast.CnosDBFunctionWithUnknownResult;
import sqlancer.cnosdb.ast.CnosDBInOperation;
import sqlancer.cnosdb.ast.CnosDBLikeOperation;
import sqlancer.cnosdb.ast.CnosDBOrderByTerm;
import sqlancer.cnosdb.ast.CnosDBPostfixOperation;
import sqlancer.cnosdb.ast.CnosDBPrefixOperation;
import sqlancer.cnosdb.ast.CnosDBPostfixOperation.PostfixOperator;
import sqlancer.cnosdb.ast.CnosDBPrefixOperation.PrefixOperator;
import sqlancer.cnosdb.ast.CnosDBAggregate;
import sqlancer.cnosdb.ast.CnosDBBetweenOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryArithmeticOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryArithmeticOperation.CnosDBBinaryOperator;
import sqlancer.cnosdb.ast.CnosDBBinaryComparisonOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.cnosdb.ast.CnosDBAggregate.CnosDBAggregateFunction;
import sqlancer.common.gen.TypedExpressionGenerator;

public class CnosDBExpressionGenerator extends TypedExpressionGenerator<CnosDBExpression, CnosDBColumn, CnosDBDataType>{
    private final CnosDBGlobalState globalState;
    private final int maxDepth;

    private CnosDBRowValue rowValue;

    private Set<CnosDBColumnValue> columnOfLeafNode;

    public CnosDBExpressionGenerator(CnosDBGlobalState globalState) {
        this.globalState = globalState;
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
    }

    public CnosDBExpressionGenerator setRowValue(CnosDBRowValue rowValue) {
        this.rowValue = rowValue;
        return this;
    }

    public void setColumnOfLeafNode(Set<CnosDBColumnValue> columnOfLeafNode) {
        this.columnOfLeafNode = columnOfLeafNode;
    }

    public void setAllowAggregates(boolean allowAggregates) {
        this.allowAggregates = allowAggregates;
    }
    
    @Override
    public CnosDBExpression generateLeafNode(CnosDBDataType type){
        if(Randomly.getBoolean()) {
            return generateConstant(type);
        } else {
            if(filterColumns(type).isEmpty()) {
                return generateConstant(type);
            }else{
                return createColumnOfType(type);
            }
        }
    }

    final List<CnosDBColumn> filterColumns(CnosDBDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type)
                    .collect(Collectors.toList());
        }
    }

    private CnosDBExpression createColumnOfType(CnosDBDataType type) {
        List<CnosDBColumn> columns = filterColumns(type);
        CnosDBColumn column = Randomly.fromList(columns);
        CnosDBConstant value = rowValue == null ? null : rowValue.getValues().get(column);
        if (columnOfLeafNode != null) {
            columnOfLeafNode.add(CnosDBColumnValue.create(column, value));
        }
        return CnosDBColumnValue.create(column, value);
    }

    public List<CnosDBExpression> generateOrderBys() {
        List<CnosDBExpression> orderBys = new ArrayList<>();
        List<CnosDBExpression> exprs = super.generateOrderBys();

        for (CnosDBExpression expr : exprs) {
            CnosDBExpression newExpr = new CnosDBOrderByTerm(expr, CnosDBOrderByTerm.CnosDBOrder.getRandomOrder());
            orderBys.add(newExpr);
        }
        return orderBys;
    }

    public List<CnosDBExpression> generateExpressions(CnosDBDataType type,int nr) {
        List<CnosDBExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(type));
        }
        return expressions;
    }

    @Override
    public CnosDBExpression generateExpression(CnosDBDataType type, int depth) {
        if (Randomly.getBooleanWithRatherLowProbability() || depth >= maxDepth) {
            return generateLeafNode(type);
        }
        switch (type) {
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case INT:
                return generateIntExpression(depth);
            case UINT:
            case TIMESTAMP:
            case STRING:
            case DOUBLE:
                // TODO: support other types
                return generateConstant(type);
            default:
                throw new AssertionError(type);
        }
    }

    public CnosDBExpression generateAggregate(CnosDBAggregateFunction func) {
        CnosDBDataType dataType = CnosDBDataType.getRandomType();
        return new CnosDBAggregate(generateExpressions(dataType, func.getNrArgs()), func);
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, LIKE, BETWEEN, IN_OPERATION,
        // FUNCTION, CAST
    } 
    
    private CnosDBExpression generateBooleanExpression(int depth) {
        if(allowAggregates) {
            allowAggregates = false;
        }

        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        BooleanExpression option = Randomly.fromList(validOptions);

        switch (option) {
            case POSTFIX_OPERATOR:
                return getPostfix(depth + 1);
            case NOT:
                return getNot(depth + 1);
            case IN_OPERATION:
                return getIn(depth + 1);
            case BETWEEN:
                return getBetween(depth + 1);
            case LIKE:
                return getLike(depth + 1, CnosDBDataType.STRING);
            case BINARY_COMPARISON:
                return getComparision(depth + 1);
            case BINARY_LOGICAL_OPERATOR:
                return getLogicalOperator(depth + 1);
            default:
                throw new AssertionError(option);
        }
    }

    private CnosDBExpression getPostfix(int depth) {
        PostfixOperator random = PostfixOperator.getRandom();
        return new CnosDBPostfixOperation(
            generateExpression(Randomly.fromOptions(random.getInputDataTypes()), depth), random
        );
    }

    private CnosDBExpression getNot(int depth) {
    PrefixOperator op = PrefixOperator.NOT;
        return new CnosDBPrefixOperation(generateExpression(CnosDBDataType.BOOLEAN, depth), op);
    }

    private CnosDBExpression getIn(int depth) {
        CnosDBDataType type = CnosDBDataType.getRandomType();
        CnosDBExpression leftExpr = generateExpression(type, depth);
        List<CnosDBExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateConstant(type));
        }
        return new CnosDBInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }

    private CnosDBExpression getBetween(int depth) {
        CnosDBDataType type = Randomly.fromList(Arrays.asList(CnosDBDataType.values()).stream().filter(t -> t != CnosDBDataType.BOOLEAN).collect(Collectors.toList()));
        return new CnosDBBetweenOperation(
            generateExpression(type, depth),
            generateExpression(type, depth), 
            generateExpression(type, depth) 
        );
    }

    private CnosDBExpression getLike(int depth, CnosDBDataType type) {
        return new CnosDBLikeOperation(
            generateExpression(type, depth), 
            generateExpression(type, depth)
        );
    }

    private CnosDBExpression getComparision(int depth) {
        CnosDBDataType dataType = CnosDBDataType.getRandomType();
        CnosDBExpression leftExpr = generateExpression(dataType, depth);
        CnosDBExpression rightExpr = generateExpression(dataType, depth);
        return new CnosDBBinaryComparisonOperation(leftExpr, rightExpr,  CnosDBBinaryComparisonOperation.CnosDBBinaryComparisonOperator.getRandom());
    }

    private CnosDBExpression getLogicalOperator(int depth) {
        CnosDBExpression first = generateExpression(CnosDBDataType.BOOLEAN, depth);
        int nr = Randomly.smallNumber() + 1;
        for(int i = 0 ; i < nr ; i++){
            first = new CnosDBBinaryLogicalOperation(first, generateExpression(CnosDBDataType.BOOLEAN, depth),
                        BinaryLogicalOperator.getRandom());
        }
        return first;
    }

    private enum IntExpression {
        UNARY_OPERATION, BINARY_ARITHMETIC_EXPRESSION, FUNCTION,
        //FUNCTION, CAST, 
    }

    private CnosDBExpression generateIntExpression(int depth) {
        if(allowAggregates) {
            allowAggregates = false;
        }

        List<IntExpression> validOptions = new ArrayList<>(Arrays.asList(IntExpression.values()));
        IntExpression option = Randomly.fromList(validOptions);

        switch (option) {
            case UNARY_OPERATION:
                return new CnosDBPrefixOperation(generateExpression(CnosDBDataType.INT, depth + 1),
                    Randomly.getBoolean() ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
            case BINARY_ARITHMETIC_EXPRESSION:
                 return new CnosDBBinaryArithmeticOperation(generateExpression(CnosDBDataType.INT, depth + 1),
                    generateExpression(CnosDBDataType.INT, depth + 1),
                    CnosDBBinaryOperator.getRandom(CnosDBDataType.INT));
            case FUNCTION:
                return generateFunction(depth + 1, CnosDBDataType.INT);
            default:
                throw new AssertionError(option);
        }
    }

    private CnosDBExpression generateFunction(int depth, CnosDBDataType returnType) {
        List<CnosDBFunctionWithUnknownResult> supportedFunctions = CnosDBFunctionWithUnknownResult
                .getSupportedFunctions(returnType);

        if (supportedFunctions.isEmpty()) {
            throw new IgnoreMeException();
        }
        CnosDBFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
        return new CnosDBFunction(randomFunction, returnType, randomFunction.getArguments(this, depth));
    }
    
    @Override
    public CnosDBExpression generateConstant(CnosDBDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return CnosDBConstant.createNullConstant();
        }
        Randomly r = globalState.getRandomly();
        switch (type) {
        case INT:
            return CnosDBConstant.createIntConstant(r.getInteger());
        case UINT:
            return CnosDBConstant.createUintConstant(r.getPositiveInteger());
        case TIMESTAMP:
            return CnosDBConstant.createTimeStampConstant(r.getPositiveIntegerNotNull());
        case BOOLEAN:
            return CnosDBConstant.createBooleanConstant(Randomly.getBoolean());
        case STRING:
            return CnosDBConstant.createStringConstant(r.getString());
        case DOUBLE:
            return CnosDBConstant.createDoubleConstant(r.getDouble());
        default:
            throw new AssertionError(type);
        }
    }

    @Override
    protected CnosDBExpression generateColumn(CnosDBDataType type) {
        return null;
    }

    @Override
    protected boolean canGenerateColumnOfType(CnosDBDataType type) {
        return false;
    }
    
    @Override
    protected CnosDBDataType getRandomType(){
        return Randomly.fromOptions(CnosDBDataType.values());
    }

    @Override
    public CnosDBExpression generatePredicate() {
        return generateExpression(CnosDBDataType.BOOLEAN);
    }

    @Override
    public CnosDBExpression negatePredicate(CnosDBExpression predicate) {
        return new CnosDBPrefixOperation(predicate, PrefixOperator.NOT);
    }

    @Override
    public CnosDBExpression isNull(CnosDBExpression expr) {
        return new CnosDBPostfixOperation(expr, PostfixOperator.IS_NULL);
    }
    
    public CnosDBExpression generateArgsForAggregate(CnosDBAggregateFunction aggregateFunction) {
        return new CnosDBAggregate(generateExpressions(aggregateFunction.getRandomType(), aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public CnosDBExpression generateAggregate() {
        CnosDBAggregateFunction aggrFunc = CnosDBAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    public CnosDBExpression generateHavingClause(){
        // TODO: support allowAggregates
        // now it's not useful 
        allowAggregates = true;
        CnosDBExpression expr = generateExpression(CnosDBDataType.BOOLEAN);
        allowAggregates = false;
        return expr;
    }
    
}
