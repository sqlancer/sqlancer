package sqlancer.mysql.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLRowValue;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLColumnValue;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLComputableFunction.MySQLFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperator;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

public class MySQLRandomExpressionGenerator {

	private final static int MAX_DEPTH = 3;
	private Randomly r;
	private List<MySQLColumn> columns;
	private MySQLGlobalState state;

	public MySQLRandomExpressionGenerator(MySQLGlobalState state) {
		this.state = state;
		this.r = state.getRandomly();
	}

	public static MySQLExpression generateRandomExpression(List<MySQLColumn> columns, MySQLRowValue rowVal,
			Randomly r) {
		MySQLExpression gen = gen(columns, rowVal, 0, r);
		return gen;
	}

	private enum Actions {
		COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, FUNCTION, BINARY_LOGICAL_OPERATOR,
		BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, /* BINARY_OPERATION, */ EXISTS, BETWEEN_OPERATOR;
	}

	public MySQLExpression generateExpression() {
		return gen(columns, null, state.getOptions().getMaxExpressionDepth(), r);
	}

	public static MySQLExpression gen(List<MySQLColumn> columns, MySQLRowValue rowVal, int depth, Randomly r) {
		if (depth >= MAX_DEPTH) {
			if (Randomly.getBoolean() && !columns.isEmpty()) {
				return generateColumn(columns, rowVal);
			} else {
				return generateLiteral(r);
			}
		}
		switch (Randomly.fromOptions(Actions.values())) {
		case COLUMN:
			return generateColumn(columns, rowVal);
		case LITERAL:
			return generateLiteral(r);
		case UNARY_PREFIX_OPERATION:
			MySQLExpression notSubExpr = gen(columns, rowVal, depth + 1, r);
			/* workaround for https://bugs.mysql.com/bug.php?id=95900 */
			while (notSubExpr instanceof MySQLUnaryPrefixOperation) {
				notSubExpr = gen(columns, rowVal, depth + 1, r);
			}
			return new MySQLUnaryPrefixOperation(notSubExpr, MySQLUnaryPrefixOperator.getRandom());
		case UNARY_POSTFIX:
			return new MySQLUnaryPostfixOperator(gen(columns, rowVal, depth + 1, r),
					Randomly.fromOptions(MySQLUnaryPostfixOperator.UnaryPostfixOperator.values()),
					Randomly.getBoolean());
		case FUNCTION:
			return getFunction(columns, rowVal, depth + 1, r);
		case BINARY_LOGICAL_OPERATOR:
			return new MySQLBinaryLogicalOperation(gen(columns, rowVal, depth + 1, r),
					gen(columns, rowVal, depth + 1, r), MySQLBinaryLogicalOperator.getRandom());
		case BINARY_COMPARISON_OPERATION:
			return new MySQLBinaryComparisonOperation(gen(columns, rowVal, depth + 1, r),
					gen(columns, rowVal, depth + 1, r), BinaryComparisonOperator.getRandom());
		case CAST:
			return new MySQLCastOperation(gen(columns, rowVal, depth + 1, r), MySQLCastOperation.CastType.getRandom());
		case IN_OPERATION:
			MySQLExpression expr = gen(columns, rowVal, depth + 1, r);
			List<MySQLExpression> rightList = new ArrayList<>();
			for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
				rightList.add(gen(columns, rowVal, depth + 1, r));
			}
			return new MySQLInOperation(expr, rightList, Randomly.getBoolean());
		/* commented out as a workaround for https://bugs.mysql.com/bug.php?id=95983 */
//		case BINARY_OPERATION:
//			return new MySQLBinaryOperation(gen(columns, rowVal, depth + 1, r), gen(columns, rowVal, depth + 1, r),
//					MySQLBinaryOperator.getRandom());
		case EXISTS:
			return getExists(columns, rowVal, depth + 1, r);
		case BETWEEN_OPERATOR:
			return new MySQLBetweenOperation(gen(columns, rowVal, depth + 1, r), gen(columns, rowVal, depth + 1, r),
					gen(columns, rowVal, depth + 1, r));
		default:
			throw new AssertionError();
		}
	}

	private static MySQLExpression getExists(List<MySQLColumn> columns, MySQLRowValue rowVal, int i, Randomly r) {
		if (Randomly.getBoolean()) {
			return new MySQLExists(new MySQLStringExpression("SELECT 1", MySQLConstant.createTrue()));
		} else {
			return new MySQLExists(new MySQLStringExpression("SELECT 1 wHERE FALSE", MySQLConstant.createFalse()));
		}
	}

	private static MySQLExpression getFunction(List<MySQLColumn> columns, MySQLRowValue rowVal, int depth, Randomly r) {
		MySQLFunction func = MySQLFunction.getRandomFunction();
		int nrArgs = func.getNrArgs();
		if (func.isVariadic()) {
			nrArgs += Randomly.smallNumber();
		}
		MySQLExpression[] args = new MySQLExpression[nrArgs];
		for (int i = 0; i < args.length; i++) {
			args[i] = gen(columns, rowVal, depth + 1, r);
		}
		return new MySQLComputableFunction(func, args);
	}

	private enum ConstantType {
		INT, NULL, STRING;
	}

	private static MySQLExpression generateLiteral(Randomly r) {
		switch (Randomly.fromOptions(ConstantType.values())) {
		case INT:
			return MySQLConstant.createIntConstant((int) r.getInteger());
		case NULL:
			return MySQLConstant.createNullConstant();
		case STRING:
			return MySQLConstant.createStringConstant(r.getString());
		default:
			throw new AssertionError();
		}
	}

	private static MySQLExpression generateColumn(List<MySQLColumn> columns, MySQLRowValue rowVal) {
		MySQLColumn c = Randomly.fromList(columns);
		MySQLConstant val;
		if (rowVal == null) {
			val = null;
		} else {
			val = rowVal.getValues().get(c);
		}
		return MySQLColumnValue.create(c, val);
	}

	public static String generateRandomExpressionString(List<MySQLColumn> columns, Object object, Randomly r) {
		MySQLExpression expr = generateRandomExpression(columns, null, r);
		return MySQLVisitor.asString(expr);
	}

	public MySQLRandomExpressionGenerator setColumns(List<MySQLColumn> columns) {
		this.columns = columns;
		return this;
	}

}
