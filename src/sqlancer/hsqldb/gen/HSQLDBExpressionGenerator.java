package sqlancer.hsqldb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBColumn;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBTable;
import sqlancer.hsqldb.ast.HSQLDBBinaryOperation;
import sqlancer.hsqldb.ast.HSQLDBColumnReference;
import sqlancer.hsqldb.ast.HSQLDBConstant;
import sqlancer.hsqldb.ast.HSQLDBExpression;
import sqlancer.hsqldb.ast.HSQLDBJoin;
import sqlancer.hsqldb.ast.HSQLDBSelect;
import sqlancer.hsqldb.ast.HSQLDBTableReference;
import sqlancer.hsqldb.ast.HSQLDBUnaryPostfixOperation;
import sqlancer.hsqldb.ast.HSQLDBUnaryPrefixOperation;

public final class HSQLDBExpressionGenerator extends
        TypedExpressionGenerator<HSQLDBExpression, HSQLDBSchema.HSQLDBColumn, HSQLDBSchema.HSQLDBCompositeDataType>
        implements NoRECGenerator<HSQLDBSelect, HSQLDBJoin, HSQLDBExpression, HSQLDBTable, HSQLDBColumn>,
        TLPWhereGenerator<HSQLDBSelect, HSQLDBJoin, HSQLDBExpression, HSQLDBTable, HSQLDBColumn> {

    List<HSQLDBTable> tables;

    private enum Expression {
        BINARY_LOGICAL, BINARY_COMPARISON, BINARY_ARITHMETIC;
    }

    HSQLDBProvider.HSQLDBGlobalState hsqldbGlobalState;

    public HSQLDBExpressionGenerator(HSQLDBProvider.HSQLDBGlobalState globalState) {
        this.hsqldbGlobalState = globalState;
    }

    @Override
    public HSQLDBExpression generatePredicate() {
        return generateExpression(
                HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithType(HSQLDBSchema.HSQLDBDataType.BOOLEAN));
    }

    @Override
    public HSQLDBExpression negatePredicate(HSQLDBExpression predicate) {
        return new HSQLDBUnaryPrefixOperation(HSQLDBUnaryPrefixOperation.HSQLDBUnaryPrefixOperator.NOT, predicate);
    }

    @Override
    public HSQLDBExpression isNull(HSQLDBExpression expr) {
        return new HSQLDBUnaryPostfixOperation(expr, HSQLDBUnaryPostfixOperation.HSQLDBUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public HSQLDBExpression generateConstant(HSQLDBSchema.HSQLDBCompositeDataType type) {
        switch (type.getType()) {
        case NULL:
            return HSQLDBConstant.createNullConstant();
        case CHAR:
            return HSQLDBConstant.HSQLDBTextConstant
                    .createStringConstant(hsqldbGlobalState.getRandomly().getAlphabeticChar(), type.getSize());
        case VARCHAR:
            return HSQLDBConstant.HSQLDBTextConstant.createStringConstant(hsqldbGlobalState.getRandomly().getString(),
                    type.getSize());
        case TIME:
            return HSQLDBConstant.createTimeConstant(
                    hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()), type.getSize());
        case TIMESTAMP:
            return HSQLDBConstant.createTimestampConstant(
                    hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()), type.getSize());

        case INTEGER:
            return HSQLDBConstant.HSQLDBIntConstant.createIntConstant(Randomly.getNonCachedInteger());
        case DOUBLE:
            return HSQLDBConstant.HSQLDBDoubleConstant.createFloatConstant(hsqldbGlobalState.getRandomly().getDouble());
        case BOOLEAN:
            return HSQLDBConstant.HSQLDBBooleanConstant.createBooleanConstant(Randomly.getBoolean());
        case DATE:
            return HSQLDBConstant
                    .createDateConstant(hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()));
        case BINARY:
            return HSQLDBConstant.createBinaryConstant(Randomly.getNonCachedInteger(), type.getSize());
        default:
            throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    protected HSQLDBExpression generateExpression(HSQLDBSchema.HSQLDBCompositeDataType type, int depth) {
        if (depth >= hsqldbGlobalState.getOptions().getMaxExpressionDepth()
                || Randomly.getBooleanWithSmallProbability()) {
            return generateLeafNode(type);
        }

        List<HSQLDBExpressionGenerator.Expression> possibleOptions = new ArrayList<>(
                Arrays.asList(HSQLDBExpressionGenerator.Expression.values()));

        HSQLDBExpressionGenerator.Expression expr = Randomly.fromList(possibleOptions);
        BinaryOperatorNode.Operator op;
        switch (expr) {
        case BINARY_LOGICAL:
        case BINARY_ARITHMETIC:
            op = HSQLDBExpressionGenerator.HSQLDBBinaryLogicalOperator.getRandom();
            break;
        case BINARY_COMPARISON:
            op = HSQLDBDBBinaryComparisonOperator.getRandom();
            break;
        default:
            throw new AssertionError();
        }

        return new HSQLDBBinaryOperation(generateExpression(type, depth + 1), generateExpression(type, depth + 1), op);

    }

    @Override
    protected HSQLDBExpression generateColumn(HSQLDBSchema.HSQLDBCompositeDataType type) {
        HSQLDBSchema.HSQLDBColumn column = Randomly
                .fromList(columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList()));
        return new HSQLDBColumnReference(column);
    }

    @Override
    protected HSQLDBSchema.HSQLDBCompositeDataType getRandomType() {
        return HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull();
    }

    @Override
    protected boolean canGenerateColumnOfType(HSQLDBSchema.HSQLDBCompositeDataType type) {
        return columns.stream().anyMatch(c -> c.getType() == type);
    }

    public enum HSQLDBBinaryLogicalOperator implements BinaryOperatorNode.Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum HSQLDBDBBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!=");

        private String textRepr;

        HSQLDBDBBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum HSQLDBDBBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), LSHIFT("<<"), RSHIFT(">>");

        private String textRepr;

        HSQLDBDBBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    @Override
    public List<HSQLDBExpression> generateOrderBys() {
        List<HSQLDBExpression> expressions = new ArrayList<>();
        int nr = Randomly.smallNumber() + 1;
        ArrayList<HSQLDBSchema.HSQLDBColumn> hsqldbColumns = new ArrayList<>(columns);
        for (int i = 0; i < nr && !hsqldbColumns.isEmpty(); i++) {
            HSQLDBSchema.HSQLDBColumn randomColumn = Randomly.fromList(hsqldbColumns);
            HSQLDBColumnReference columnReference = new HSQLDBColumnReference(randomColumn);
            hsqldbColumns.remove(randomColumn);
            expressions.add(columnReference);
        }
        return expressions;
    }

    @Override
    public HSQLDBExpressionGenerator setTablesAndColumns(AbstractTables<HSQLDBTable, HSQLDBColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public HSQLDBExpression generateBooleanExpression() {
        return generatePredicate();
    }

    @Override
    public HSQLDBSelect generateSelect() {
        return new HSQLDBSelect();
    }

    @Override
    public List<HSQLDBJoin> getRandomJoinClauses() {
        List<HSQLDBJoin> joinExpressions = new ArrayList<>();
        while (tables.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            HSQLDBTable leftTable = tables.remove(0);
            HSQLDBTable rightTable = tables.remove(0);
            List<HSQLDBSchema.HSQLDBColumn> columns = new ArrayList<>(leftTable.getColumns());
            columns.addAll(rightTable.getColumns());
            HSQLDBExpressionGenerator joinGen = new HSQLDBExpressionGenerator(hsqldbGlobalState).setColumns(columns);
            HSQLDBTableReference leftTableRef = new HSQLDBTableReference(leftTable);
            HSQLDBTableReference rightTableRef = new HSQLDBTableReference(rightTable);
            switch (HSQLDBJoin.JoinType.getRandomForDatabase("HSQLDB")) {
            case INNER:
                joinExpressions.add(HSQLDBJoin.createInnerJoin(leftTableRef, rightTableRef,
                        joinGen.generateExpression(HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull())));
                break;
            case NATURAL:
                joinExpressions.add(
                        HSQLDBJoin.createNaturalJoin(leftTableRef, rightTableRef, HSQLDBJoin.OuterType.getRandom()));
                break;
            case LEFT:
                joinExpressions.add(HSQLDBJoin.createLeftOuterJoin(leftTableRef, rightTableRef,
                        joinGen.generateExpression(HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull())));
                break;
            case RIGHT:
                joinExpressions.add(HSQLDBJoin.createRightOuterJoin(leftTableRef, rightTableRef,
                        joinGen.generateExpression(HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull())));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    @Override
    public List<HSQLDBExpression> getTableRefs() {
        return tables.stream().map(t -> new HSQLDBTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public String generateOptimizedQueryString(HSQLDBSelect select, HSQLDBExpression whereCondition,
            boolean shouldUseAggregate) {
        if (shouldUseAggregate) {
            HSQLDBColumn aggr = new HSQLDBColumn("COUNT(*)", null, null);
            select.setFetchColumns(List.of(new HSQLDBColumnReference(aggr)));
        } else {
            List<HSQLDBExpression> allColumns = columns.stream().map((c) -> new HSQLDBColumnReference(c))
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
    public String generateUnoptimizedQueryString(HSQLDBSelect select, HSQLDBExpression whereCondition) {
        HSQLDBColumn c = new HSQLDBColumn("COUNT(*) as count", null, null);
        select.setFetchColumns(List.of(new HSQLDBColumnReference(c)));
        select.setWhereClause(null);
        return "SELECT SUM(count) FROM (" + select.asString() + ") as res";
    }

    @Override
    public List<HSQLDBExpression> generateFetchColumns(boolean shouldCreateDummy) {
        if (shouldCreateDummy) {
            return List.of(new HSQLDBColumnReference(new HSQLDBSchema.HSQLDBColumn("*", null, null)));
        }
        return Randomly
                .nonEmptySubset(columns.stream().map(c -> new HSQLDBColumnReference(c)).collect(Collectors.toList()));
    }
}
