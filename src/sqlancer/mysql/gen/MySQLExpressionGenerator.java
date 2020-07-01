package sqlancer.mysql.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.gen.UntypedExpressionGenerator;
import sqlancer.mysql.MySQLBugs;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLRowValue;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation.MySQLBinaryOperator;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLComputableFunction.MySQLFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLConstant.MySQLDoubleConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

public class MySQLExpressionGenerator extends UntypedExpressionGenerator<MySQLExpression, MySQLColumn> {

    private final MySQLGlobalState state;
    private MySQLRowValue rowVal;

    public MySQLExpressionGenerator(MySQLGlobalState state) {
        this.state = state;
    }

    public MySQLExpressionGenerator setRowVal(MySQLRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION, BINARY_LOGICAL_OPERATOR,
        BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, BINARY_OPERATION, EXISTS, BETWEEN_OPERATOR;
    }

    @Override
    public MySQLExpression generateExpression(int depth) {
        if (depth >= state.getOptions().getMaxExpressionDepth()) {
            return generateLeafNode();
        }
        switch (Randomly.fromOptions(Actions.values())) {
        case COLUMN:
            return generateColumn();
        case LITERAL:
            return generateConstant();
        case UNARY_PREFIX_OPERATION:
            MySQLExpression subExpr = generateExpression(depth + 1);
            MySQLUnaryPrefixOperator random = MySQLUnaryPrefixOperator.getRandom();
            if (random == MySQLUnaryPrefixOperator.MINUS) {
                // workaround for https://bugs.mysql.com/bug.php?id=99122
                throw new IgnoreMeException();
            }
            return new MySQLUnaryPrefixOperation(subExpr, random);
        case UNARY_POSTFIX:
            return new MySQLUnaryPostfixOperation(generateExpression(depth + 1),
                    Randomly.fromOptions(MySQLUnaryPostfixOperation.UnaryPostfixOperator.values()),
                    Randomly.getBoolean());
        case COMPUTABLE_FUNCTION:
            return getComputableFunction(depth + 1);
        case BINARY_LOGICAL_OPERATOR:
            return new MySQLBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    MySQLBinaryLogicalOperator.getRandom());
        case BINARY_COMPARISON_OPERATION:
            return new MySQLBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    BinaryComparisonOperator.getRandom());
        case CAST:
            return new MySQLCastOperation(generateExpression(depth + 1), MySQLCastOperation.CastType.getRandom());
        case IN_OPERATION:
            MySQLExpression expr = generateExpression(depth + 1);
            List<MySQLExpression> rightList = new ArrayList<>();
            for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                rightList.add(generateExpression(depth + 1));
            }
            return new MySQLInOperation(expr, rightList, Randomly.getBoolean());
        case BINARY_OPERATION:
            if (MySQLBugs.bug99135) {
                throw new IgnoreMeException();
            }
            return new MySQLBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    MySQLBinaryOperator.getRandom());
        case EXISTS:
            return getExists();
        case BETWEEN_OPERATOR:
            if (MySQLBugs.bug99181) {
                // TODO: there are a number of bugs that are triggered by the BETWEEN operator
                throw new IgnoreMeException();
            }
            return new MySQLBetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1));
        default:
            throw new AssertionError();
        }
    }

    private MySQLExpression getExists() {
        if (Randomly.getBoolean()) {
            return new MySQLExists(new MySQLStringExpression("SELECT 1", MySQLConstant.createTrue()));
        } else {
            return new MySQLExists(new MySQLStringExpression("SELECT 1 wHERE FALSE", MySQLConstant.createFalse()));
        }
    }

    private MySQLExpression getComputableFunction(int depth) {
        MySQLFunction func = MySQLFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        MySQLExpression[] args = new MySQLExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = generateExpression(depth + 1);
        }
        return new MySQLComputableFunction(func, args);
    }

    private enum ConstantType {
        INT, NULL, STRING, DOUBLE;
    }

    @Override
    public MySQLExpression generateConstant() {
        switch (Randomly.fromOptions(ConstantType.values())) {
        case INT:
            return MySQLConstant.createIntConstant((int) state.getRandomly().getInteger());
        case NULL:
            return MySQLConstant.createNullConstant();
        case STRING:
            String string = state.getRandomly().getString();
            if (string.startsWith("\n")) {
                // workaround for https://bugs.mysql.com/bug.php?id=99130
                throw new IgnoreMeException();
            }
            if (string.startsWith("-0") || string.startsWith("0.") || string.startsWith(".")) {
                // https://bugs.mysql.com/bug.php?id=99145
                throw new IgnoreMeException();
            }
            MySQLConstant createStringConstant = MySQLConstant.createStringConstant(string);
            // if (Randomly.getBoolean()) {
            // return new MySQLCollate(createStringConstant, Randomly.fromOptions("ascii_bin", "binary"));
            // }
            if (string.startsWith("1e")) {
                // https://bugs.mysql.com/bug.php?id=99146
                throw new IgnoreMeException();
            }
            return createStringConstant;
        case DOUBLE:
            double val = state.getRandomly().getDouble();
            if (Math.abs(val) <= 1 && val != 0) {
                // https://bugs.mysql.com/bug.php?id=99145
                throw new IgnoreMeException();
            }
            if (Math.abs(val) > 1.0E30) {
                // https://bugs.mysql.com/bug.php?id=99146
                throw new IgnoreMeException();
            }
            return new MySQLDoubleConstant(val);
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected MySQLExpression generateColumn() {
        MySQLColumn c = Randomly.fromList(columns);
        MySQLConstant val;
        if (rowVal == null) {
            val = null;
        } else {
            val = rowVal.getValues().get(c);
        }
        return MySQLColumnReference.create(c, val);
    }

}
