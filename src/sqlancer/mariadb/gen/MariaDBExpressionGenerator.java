package sqlancer.mariadb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StateToReproduce;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBDataType;
import sqlancer.mariadb.ast.MariaDBBinaryOperator;
import sqlancer.mariadb.ast.MariaDBBinaryOperator.MariaDBBinaryComparisonOperator;
import sqlancer.mariadb.ast.MariaDBColumnName;
import sqlancer.mariadb.ast.MariaDBConstant;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBFunction;
import sqlancer.mariadb.ast.MariaDBFunctionName;
import sqlancer.mariadb.ast.MariaDBInOperation;
import sqlancer.mariadb.ast.MariaDBPostfixUnaryOperation;
import sqlancer.mariadb.ast.MariaDBPostfixUnaryOperation.MariaDBPostfixUnaryOperator;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation.MariaDBUnaryPrefixOperator;

public class MariaDBExpressionGenerator {

    private final Randomly r;
    private List<MariaDBColumn> columns = new ArrayList<>();

    public MariaDBExpressionGenerator(Randomly r) {
        this.r = r;
    }

    public static MariaDBConstant getRandomConstant(Randomly r) {
        MariaDBDataType option = Randomly.fromOptions(MariaDBDataType.values());
        return getRandomConstant(r, option);
    }

    public static MariaDBConstant getRandomConstant(Randomly r, MariaDBDataType option) throws AssertionError {
        if (Randomly.getBooleanWithSmallProbability()) {
            return MariaDBConstant.createNullConstant();
        }
        switch (option) {
        case REAL:
            // FIXME: bug workaround for MDEV-21032
            return MariaDBConstant.createIntConstant(r.getInteger());
        // double val;
        // do {
        // val = r.getDouble();
        // } while (Double.isInfinite(val));
        // return MariaDBConstant.createDoubleConstant(val);
        case INT:
            return MariaDBConstant.createIntConstant(r.getInteger());
        case VARCHAR:
            return MariaDBConstant.createTextConstant(r.getString());
        case BOOLEAN:
            return MariaDBConstant.createBooleanConstant(Randomly.getBoolean());
        default:
            throw new AssertionError(option);
        }
    }

    public MariaDBExpressionGenerator setColumns(List<MariaDBColumn> columns) {
        this.columns = columns;
        return this;
    }

    public MariaDBExpressionGenerator setCon(SQLConnection con) {
        return this;
    }

    public MariaDBExpressionGenerator setState(StateToReproduce state) {
        return this;
    }

    private enum ExpressionType {
        LITERAL, COLUMN, BINARY_COMPARISON, UNARY_POSTFIX_OPERATOR, UNARY_PREFIX_OPERATOR, FUNCTION, IN
    }

    public MariaDBExpression getRandomExpression(int depth) {
        if (depth >= MariaDBProvider.MAX_EXPRESSION_DEPTH || Randomly.getBoolean()) {
            if (Randomly.getBoolean() || columns.isEmpty()) {
                return getRandomConstant(r);
            } else {
                return getRandomColumn();
            }
        }
        List<ExpressionType> expressionTypes = new ArrayList<>(Arrays.asList(ExpressionType.values()));
        if (columns.isEmpty()) {
            expressionTypes.remove(ExpressionType.COLUMN);
        }
        ExpressionType expressionType = Randomly.fromList(expressionTypes);
        switch (expressionType) {
        case COLUMN:
            getRandomColumn();
        case LITERAL:
            return getRandomConstant(r);
        case BINARY_COMPARISON:
            return new MariaDBBinaryOperator(getRandomExpression(depth + 1), getRandomExpression(depth + 1),
                    MariaDBBinaryComparisonOperator.getRandom());
        case UNARY_PREFIX_OPERATOR:
            return new MariaDBUnaryPrefixOperation(getRandomExpression(depth + 1),
                    MariaDBUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX_OPERATOR:
            return new MariaDBPostfixUnaryOperation(MariaDBPostfixUnaryOperator.getRandom(),
                    getRandomExpression(depth + 1));
        case FUNCTION:
            MariaDBFunctionName func = MariaDBFunctionName.getRandom();
            return new MariaDBFunction(func, getArgs(func, depth + 1));
        case IN:
            return new MariaDBInOperation(getRandomExpression(depth + 1), getSmallNumberRandomExpressions(depth + 1),
                    Randomly.getBoolean());
        default:
            throw new AssertionError(expressionType);
        }
    }

    private List<MariaDBExpression> getSmallNumberRandomExpressions(int depth) {
        List<MariaDBExpression> expressions = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            expressions.add(getRandomExpression(depth + 1));
        }
        return expressions;
    }

    private List<MariaDBExpression> getArgs(MariaDBFunctionName func, int depth) {
        List<MariaDBExpression> expressions = new ArrayList<>();
        for (int i = 0; i < func.getNrArgs(); i++) {
            expressions.add(getRandomExpression(depth + 1));
        }
        if (func.isVariadic()) {
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                expressions.add(getRandomExpression(depth + 1));
            }
        }
        return expressions;
    }

    private MariaDBExpression getRandomColumn() {
        MariaDBColumn randomColumn = Randomly.fromList(columns);
        return new MariaDBColumnName(randomColumn);
    }

    public MariaDBExpression getRandomExpression() {
        return getRandomExpression(0);
    }

}
