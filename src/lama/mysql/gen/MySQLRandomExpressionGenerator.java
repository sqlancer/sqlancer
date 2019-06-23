package lama.mysql.gen;

import java.util.ArrayList;
import java.util.List;

import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.MySQLSchema.MySQLRowValue;
import lama.mysql.ast.MySQLBinaryComparisonOperation;
import lama.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import lama.mysql.ast.MySQLBinaryLogicalOperation;
import lama.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import lama.mysql.ast.MySQLCastOperation;
import lama.mysql.ast.MySQLColumnValue;
import lama.mysql.ast.MySQLComputableFunction;
import lama.mysql.ast.MySQLComputableFunction.MySQLFunction;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.mysql.ast.MySQLInOperation;
import lama.mysql.ast.MySQLUnaryPostfixOperator;
import lama.mysql.ast.MySQLUnaryPrefixOperation;
import lama.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

public class MySQLRandomExpressionGenerator {

	private final static int MAX_DEPTH = 1;

	public static MySQLExpression generateRandomExpression(List<MySQLColumn> columns, MySQLRowValue rowVal,
			Randomly r) {
		return gen(columns, rowVal, 0, r);
	}

	private enum Actions {
		COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, FUNCTION, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION;
	}

	public static MySQLExpression gen(List<MySQLColumn> columns, MySQLRowValue rowVal, int depth, Randomly r) {
		if (depth > MAX_DEPTH) {
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
			return new MySQLInOperation(expr, rightList);
		default:
			throw new AssertionError();
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
			do {
				args[i] = gen(columns, rowVal, depth + 1, r);
				/* workaround for https://bugs.mysql.com/bug.php?id=95926 */
			} while (args[i] instanceof MySQLConstant);
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

}
