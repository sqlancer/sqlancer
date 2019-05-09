package lama;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lama.Expression.BinaryOperation;
import lama.Expression.BinaryOperation.BinaryOperator;
import lama.Expression.Cast;
import lama.Expression.ColumnName;
import lama.Expression.Constant;
import lama.Expression.Function;
import lama.Expression.OrderingTerm;
import lama.Expression.OrderingTerm.Ordering;
import lama.Expression.PostfixUnaryOperation;
import lama.Expression.TypeLiteral;
import lama.Expression.TypeLiteral.Type;
import lama.Expression.UnaryOperation;
import lama.Expression.UnaryOperation.UnaryOperator;
import lama.Main.StateToReproduce;
import lama.schema.SQLite3DataType;
import lama.schema.Schema;
import lama.schema.Schema.Column;
import lama.schema.Schema.RowValue;
import lama.schema.Schema.Table;
import lama.schema.Schema.Tables;
import lama.sqlite3.SQLite3Visitor;
import lama.tablegen.sqlite3.SQLite3ExpressionGenerator;

public class QueryGenerator {

	private final Connection database;
	private final Schema s;

	public QueryGenerator(Connection con) throws SQLException {
		this.database = con;
		s = Schema.fromConnection(database);
	}

	public void generateAndCheckQuery(StateToReproduce state) throws SQLException {
		Tables tables = s.getRandomTableNonEmptyTables();
		state.queryTargetedTablesString = tables.tableNamesAsString();
		SelectStatement selectStatement = new SelectStatement();
		selectStatement.setSelectType(Randomly.fromOptions(SelectStatement.SelectType.values()));
		selectStatement.setFromTables(tables.getTables());
		List<Column> columns = tables.getColumns();
		for (Table t : tables.getTables()) {
			if (t.getRowid() != null) {
				columns.add(t.getRowid());
			}
		}
		List<Column> fetchColumns;
		RowValue rw = tables.getRandomRowValue(database, state);
		// TODO: also implement a wild-card check (*)
		fetchColumns = Randomly.nonEmptySubset(columns);
		selectStatement.selectFetchColumns(fetchColumns);
		state.queryTargetedColumnsString = fetchColumns.stream().map(c -> c.getFullQualifiedName())
				.collect(Collectors.joining(", "));
		Expression whereClause = generateWhereClauseThatContainsRowValue(columns, rw);
		selectStatement.setWhereClause(whereClause);
		List<Expression> groupByClause = generateGroupByClause(columns);
		selectStatement.setGroupByClause(groupByClause);
		Expression limitClause = generateLimit();
		selectStatement.setLimitClause(limitClause);
		if (limitClause != null) {
			Expression offsetClause = generateOffset();
			selectStatement.setOffsetClause(offsetClause);
		}
		List<Expression> orderBy = generateOrderBy(columns, rw);
		selectStatement.setOrderByClause(orderBy);
		SQLite3Visitor visitor = new SQLite3Visitor();
		visitor.visit(selectStatement);
		String queryString = visitor.get();
		boolean isContainedIn = isContainedIn(state, rw, fetchColumns, queryString);
		if (!isContainedIn) {
			throw new Main.ReduceMeException();
		}
	}

	private Expression generateOffset() {
		if (Randomly.getBoolean()) {
			// OFFSET 0
			return Constant.createIntConstant(0);
		} else {
			return null;
		}
	}

	private boolean isContainedIn(StateToReproduce state, RowValue rw, List<Column> fetchColumns, String queryString)
			throws SQLException {
		Statement createStatement;
		createStatement = database.createStatement();

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		String columnNames = rw.getRowValuesAsString(fetchColumns);
		sb.append(columnNames);
		state.values = columnNames;
		sb.append(" INTERSECT SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
		sb.append(queryString);
		sb.append(")");
		String resultingQueryString = sb.toString();
		state.query = resultingQueryString;
		try (ResultSet result = createStatement.executeQuery(resultingQueryString)) {
			boolean isContainedIn = !result.isClosed();
			createStatement.close();
			return isContainedIn;
		} catch (SQLException e) {
			if (shouldIgnoreException(e)) {
				return true;
			} else {
				throw e;
			}
		}
	}

	static boolean shouldIgnoreException(SQLException e) {
		return e.getMessage().contentEquals("[SQLITE_ERROR] SQL error or missing database (integer overflow)");
	}

	private List<Expression> generateOrderBy(List<Column> columns, RowValue rw) {
		List<Expression> orderBys = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			Expression expr;
			expr = Constant.createTextConstant(Randomly.getString());
			Ordering order = Randomly.fromOptions(Ordering.ASC, Ordering.DESC);
			orderBys.add(new OrderingTerm(expr, order));
			// TODO RANDOM()
		}
		// TODO collate
		return orderBys;
	}

	private boolean integerConstantOutsideRange(Expression expr, int size) {
		if (!(expr instanceof Constant)) {
			return false;
		}
		Constant con = (Constant) expr;
		if (con.isNull()) {
			return false;
		}
		if (con.getDataType() == SQLite3DataType.INT) {
			long val = con.asInt();
			if (val < 1 || val > size) {
				return false;
			}
		}
		return true;
	}

	private Expression generateLimit() {
		if (Randomly.getBoolean()) {
			return Constant.createIntConstant(Integer.MAX_VALUE);
		} else {
			return null;
		}
	}

	private List<Expression> generateGroupByClause(List<Column> columns) {
		if (Randomly.getBoolean()) {
			return columns.stream().map(c -> new ColumnName(c)).collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	private Expression generateWhereClauseThatContainsRowValue(List<Column> columns, RowValue rw) {

		Expression whereClause = generateNewExpression(columns, rw, true, 0);

		return whereClause;
	}

	private enum NewExpressionType {
		CAST_TO_ITSELF, LITERAL, STANDALONE_COLUMN, DOUBLE_COLUMN, POSTFIX_COLUMN, NOT, UNARY_PLUS, AND, OR, COLLATE,
		UNARY_FUNCTION, IN, ALWAYS_TRUE_COLUMN_COMPARISON, CAST_TO_NUMERIC
	}

	private Expression generateNewExpression(List<Column> columns, RowValue rw, boolean shouldBeTrue, int depth) {
		if (depth >= Main.EXPRESSION_MAX_DEPTH) {
			return getStandaloneLiteral(shouldBeTrue);
		}
		if (Randomly.getBoolean()) {
			Column c = Randomly.fromList(columns);
			Constant sampledConstant = rw.getValues().get(c);
			return createSampleBasedTwoColumnComparison(columns, rw, sampledConstant, new ColumnName(c), shouldBeTrue);
		}
		if (Randomly.getBoolean() && shouldBeTrue) {
			return generateExpression(columns, rw);
		}
		boolean retry;
		do {
			retry = false;
			switch (Randomly.fromOptions(NewExpressionType.values())) {
			case CAST_TO_NUMERIC:
				Expression subExpr2 = createStandaloneColumn(columns, rw, true);
				if (subExpr2 == null) {
					retry = true;
					break;
				} else {
					Constant value = rw.getValues().get(((ColumnName) subExpr2).getColumn());
					assert !value.isNull();
					TypeLiteral typeofExpr = new Expression.TypeLiteral(Type.NUMERIC);
					Cast castExpr = new Expression.Cast(typeofExpr, value);
					Constant castConstant = castToNumeric(value);
					assert castConstant != null;
					return new BinaryOperation(castExpr, castConstant,
							shouldBeTrue ? BinaryOperator.EQUALS : BinaryOperator.NOT_EQUALS);
				}
			case ALWAYS_TRUE_COLUMN_COMPARISON:
				Expression alwaysTrue = createAlwaysTrueColumnComparison(columns, rw, shouldBeTrue);
				if (alwaysTrue == null) {
					retry = true;
					continue;
				} else {
					return alwaysTrue;
				}
			case CAST_TO_ITSELF: // TODO: let each expression have a type so that CAST can be applied without
									// typeof
				Expression subExpr = createStandaloneColumn(columns, rw, shouldBeTrue);
				if (subExpr == null) {
					retry = true;
					break;
				}
				ColumnName column = (ColumnName) subExpr;
				assert rw.getValues().get(column.getColumn()) != null;
				Expression.TypeLiteral.Type type;
				switch (rw.getValues().get(column.getColumn()).getDataType()) {
				case NONE:
					throw new AssertionError();
				case BINARY:
					type = Type.BINARY;
					break;
				case INT:
					type = Type.INTEGER;
					break;
				case NULL:
					type = Randomly.fromOptions(Type.values());
				case REAL:
					type = Type.REAL;
					break;
				case TEXT:
					type = Type.TEXT;
					break;
				default:
					throw new AssertionError();
				}
				TypeLiteral typeofExpr = new Expression.TypeLiteral(type);
				return new Expression.Cast(typeofExpr, subExpr);
			case STANDALONE_COLUMN:
				Expression expr = createStandaloneColumn(columns, rw, shouldBeTrue);
				if (expr == null) {
					retry = true;
					continue;
				} else {
					return expr;
				}
			case DOUBLE_COLUMN:
				Column c = Randomly.fromList(columns);
				Constant sampledConstant = rw.getValues().get(c);
				return createSampleBasedTwoColumnComparison(columns, rw, sampledConstant, new ColumnName(c),
						shouldBeTrue);
			case POSTFIX_COLUMN:
				c = Randomly.fromList(columns);
				sampledConstant = rw.getValues().get(c);
				return generateSampleBasedColumnPostfix(sampledConstant, new ColumnName(c), shouldBeTrue);
			case LITERAL:
				return getStandaloneLiteral(shouldBeTrue);
			case UNARY_FUNCTION:
				if (shouldBeTrue) {
					return getUnaryFunction();
				} else {
					retry = true;
					break;
				}
			case NOT:
				return new UnaryOperation(UnaryOperator.NOT,
						generateNewExpression(columns, rw, !shouldBeTrue, depth + 1));
			case UNARY_PLUS:
				return new UnaryOperation(UnaryOperator.PLUS,
						generateNewExpression(columns, rw, shouldBeTrue, depth + 1));
			case COLLATE:
				return new Expression.CollateOperation(generateNewExpression(columns, rw, shouldBeTrue, depth + 1),
						Randomly.fromOptions("NOCASE", "RTRIM", "BINARY"));
			case AND:
				Expression left;
				Expression right;
				left = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
				if (shouldBeTrue) {
					right = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
					return new Expression.BinaryOperation(left, right, BinaryOperator.AND);
				} else {
					right = generateNewExpression(columns, rw, Randomly.getBoolean(), depth + 1);
					return new Expression.BinaryOperation(left, right, BinaryOperator.AND);
				}
			case OR:
				Expression leftExpr = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
				Expression rightExpr;
				if (shouldBeTrue) {
					// one side can be false
					if (Randomly.getBoolean()) {
						rightExpr = generateNewExpression(columns, rw, Randomly.getBoolean(), depth + 1);
					} else {
						rightExpr = SQLite3ExpressionGenerator.getRandomExpression(columns, false);
					}

					if (Randomly.getBoolean()) {
						// swap to allow leftExpr to be false
						Expression tmpExpression = leftExpr;
						leftExpr = rightExpr;
						rightExpr = tmpExpression;
					}
					return new Expression.BinaryOperation(leftExpr, rightExpr, BinaryOperator.OR);
				} else {
					rightExpr = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
					return new Expression.BinaryOperation(leftExpr, rightExpr, BinaryOperator.OR);
				}
			case IN:
				c = Randomly.fromList(columns);
				List<Expression> expressions = new ArrayList<>();
				if (rw.getValues().get(c).isNull()) {
					// NULL in (NULL) returns null;
					// also NULL NOT IN (NULL) returns null.
					retry = true;
					break;
				} else {
					if (shouldBeTrue) {
						if (Randomly.getBoolean()) {
							// generate random expressions and add either the column or value
							for (int i = 0; i < Randomly.smallNumber(); i++) {
								expressions
										.add(SQLite3ExpressionGenerator.getRandomExpression(columns, depth + 1, false));
							}
							int randomPosition = Randomly.getInteger(0, expressions.size());
							if (Randomly.getBoolean()) {
								expressions.add(randomPosition, new Expression.ColumnName(c));
							} else {
								expressions.add(randomPosition, rw.getValues().get(c));
							}
						} else {
							String query = "SELECT " + c.getName() + " FROM " + c.getTable().getName();
							expressions.add(Expression.Subquery.create(query));
						}
					} else {
						for (int i = 0; i < Randomly.smallNumber(); i++) {
							expressions.add(notEqualConstant(rw.getValues().get(c)));
							// TODO also not equals column
						}
					}
				}
				return new Expression.InOperation(new Expression.ColumnName(c), expressions);
			default:
				throw new AssertionError();
			}
		} while (retry);
		throw new AssertionError();
	}

	private Expression createAlwaysTrueColumnComparison(List<Column> columns, RowValue rw, boolean shouldBeTrue) {
		Column left = Randomly.fromList(columns);
		Column right = Randomly.fromList(columns);
		return createAlwaysTrueTwoColumnExpression(left, right, rw, shouldBeTrue);
	}

	private Expression getUnaryFunction() {
		String functionName = Randomly.fromOptions("sqlite_source_id", "sqlite_version",
				"total_changes" /* some rows should have been inserted */);
		return new Expression.Function(functionName);
	}

	/**
	 * Applies numeric affinity to a value.
	 */
	public static Constant castToNumeric(Constant value) {
		if (value.getDataType() == SQLite3DataType.BINARY) {
			String text = new String(value.asBinary());
			value = Constant.createTextConstant(text);
		}
		switch (value.getDataType()) {
		case INT:
		case REAL:
			return value;
		case TEXT:
			if (!value.asString().isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(value.asString())) {
				return Constant.createIntConstant(0);
			}
			for (int i = value.asString().length(); i >= 0; i--) {
				try {
					double d = Double.valueOf(value.asString().substring(0, i));
//					if (value.asString().equals("-0.0")) {
//						return Constant.createRealConstant(0.0);
//					} else
					if (d == (int) d) {
						return Constant.createIntConstant((long) d);
					} else {
						return Constant.createRealConstant(d);
					}
				} catch (Exception e) {

				}
			}
			return Constant.createIntConstant(0);
		default:
			throw new AssertionError(value);
		}
	}

	private final static byte FILE_SEPARATOR = 0x1c;
	private final static byte GROUP_SEPARATOR = 0x1d;
	private final static byte RECORD_SEPARATOR = 0x1e;
	private final static byte UNIT_SEPARATOR = 0x1f;

	private static boolean unprintAbleCharThatLetsBecomeNumberZero(String s) {
		// non-printable characters are ignored by Double.valueOf
		for (int i = 0; i < s.length(); i++) {
			char charAt = s.charAt(i);
			if (!Character.isISOControl(charAt)) {
				return false;
			}
			switch (charAt) {
			case GROUP_SEPARATOR:
			case FILE_SEPARATOR:
			case RECORD_SEPARATOR:
			case UNIT_SEPARATOR:
				return true;
			}

			if (Character.isWhitespace(charAt)) {
				continue;
			} else {
				return true;
			}
		}
		return false;
	}

	// e.g., WHERE c0 or
	private Expression createStandaloneColumn(List<Column> columns, RowValue rw, boolean shouldBeTrue) {
		Column c = Randomly.fromList(columns);
		Constant value = rw.getValues().get(c);
		if (value.isNull()) {
			return null;
		}
		Constant numericValue;
		if (value.getDataType() == SQLite3DataType.TEXT || value.getDataType() == SQLite3DataType.BINARY) {
			numericValue = castToNumeric(value);
		} else {
			numericValue = value;
		}
		assert numericValue.getDataType() != SQLite3DataType.TEXT : numericValue + "should have been converted";
		switch (numericValue.getDataType()) {
		case INT:
			if (shouldBeTrue && numericValue.asInt() != 0) {
				return new ColumnName(c);
			} else if (!shouldBeTrue && numericValue.asInt() == 0) {
				return new ColumnName(c);
			} else {
				return null;
			}
		case REAL:
			// TODO: directly comparing to a double is probably not a good idea
			if (shouldBeTrue && numericValue.asDouble() != 0.0) {
				return new ColumnName(c);
			} else if (!shouldBeTrue && numericValue.asDouble() == 0.0) {
				return new ColumnName(c);
			} else {
				return null;
			}
		default:
			throw new AssertionError(numericValue);
		}
	}

	private Expression getStandaloneLiteral(boolean shouldBeTrue) throws AssertionError {
		switch (Randomly.fromOptions(SQLite3DataType.INT, SQLite3DataType.TEXT, SQLite3DataType.REAL)) {
		case INT:
			// only a zero integer is false
			long value;
			if (shouldBeTrue) {
				value = Randomly.getNonZeroInteger();
			} else {
				value = 0;
			}
			return Constant.createIntConstant(value);
		case TEXT:
			String strValue;
			if (shouldBeTrue) {
				strValue = Randomly.getNonZeroString();
			} else {
				strValue = Randomly.fromOptions("0", "asdf", "c", "-a");
			}
			return Constant.createTextConstant(strValue);
		case REAL:
			double realValue;
			if (shouldBeTrue) {
				realValue = Randomly.getNonZeroReal();
			} else {
				realValue = Randomly.fromOptions(0.0, -0.0);
			}
			return Constant.createRealConstant(realValue);
		default:
			throw new AssertionError();
		}
	}

	private Expression generateExpression(List<Column> columns, RowValue rw) throws AssertionError {
		Expression term;
		BinaryOperator operator = Randomly.fromOptions(BinaryOperator.EQUALS, BinaryOperator.GREATER_EQUALS,
				BinaryOperator.IS, BinaryOperator.SMALLER_EQUALS);
		if (Randomly.getBoolean()) {
			// generate opaque predicate
			Expression con;
			SQLite3DataType randomType = Randomly.fromOptions(SQLite3DataType.INT, SQLite3DataType.TEXT,
					SQLite3DataType.NULL);

			switch (randomType) {
			case INT:
				long val = Randomly.getInteger();
				con = Constant.createIntConstant(val);
				break;
			case TEXT:
				con = Constant.createTextConstant(Randomly.getString());
				break;
			case NULL:
				con = Constant.createNullConstant();
				operator = BinaryOperator.IS;
				break;
			default:
				throw new AssertionError();
			}

			if (Randomly.getBoolean()) {
				// apply function
				con = new Expression.Function(getRandomUnaryFunction(), con);
			}
			term = new Expression.BinaryOperation(con, con, operator);
		} else {
			// select column
			Column selectedColumn = Randomly.fromList(columns);
			Constant sampledConstant = rw.getValues().get(selectedColumn);

			Expression compareTo;
			BinaryOperator binaryOperator;
			SQLite3DataType valueType = sampledConstant.getDataType();

			Expression columnName = new Expression.ColumnName(selectedColumn);
			if (Randomly.getBoolean()) {
				term = generateSampleBasedColumnPostfix(sampledConstant, columnName, true);
			} else {
				if (sampledConstant.isNull()) {
					// is null, comparison with >= etc. yields false
					binaryOperator = BinaryOperator.IS;
					compareTo = Randomly.fromOptions(sampledConstant, columnName);
				} else if (Randomly.getBoolean()) {
					return createSampleBasedTwoColumnComparison(columns, rw, sampledConstant, columnName, true);
				} else {
					Tuple t = createSampleBasedColumnConstantComparison(sampledConstant, columnName);
					compareTo = t.expr;
					binaryOperator = t.op;
				}
				assert compareTo != null : binaryOperator;
				if (Randomly.getBoolean() && binaryOperator != BinaryOperator.LIKE
						&& binaryOperator != BinaryOperator.NOT_EQUALS) {
					String function = getRandomFunction(binaryOperator, true, valueType, valueType);
					if (function != null) {
						// apply function
						columnName = new Expression.Function(function, columnName);
						compareTo = new Expression.Function(function, compareTo);
					}
				}
				term = new Expression.BinaryOperation(columnName, compareTo, binaryOperator);
			}
		}
		return term;
	}

	class Tuple {
		public Tuple(Expression compareTo, BinaryOperator binaryOperator) {
			this.op = binaryOperator;
			this.expr = compareTo;
		}

		BinaryOperator op;
		Expression expr;
	}

	private Expression createAlwaysTrueTwoColumnExpression(Column left, Column right, RowValue rw,
			boolean shouldBeTrue) {
		BinaryOperator binaryOperator = Randomly.fromOptions(BinaryOperator.GREATER_EQUALS,
				BinaryOperator.SMALLER_EQUALS, BinaryOperator.IS, BinaryOperator.NOT_EQUALS, BinaryOperator.GREATER,
				BinaryOperator.SMALLER, BinaryOperator.EQUALS);
		Expression leftColumn = new ColumnName(left);
		Expression rightColumn = new ColumnName(right);
		if (rw.getValues().get(left).isNull() || rw.getValues().get(right).isNull()) {
			// TODO add another OR ISNULL clause
			return null;
		}
		if (Randomly.getBoolean()) {
			BinaryFunction func;
			do {
				func = Randomly.fromOptions(BinaryFunction.values());
			} while (func == BinaryFunction.UNICODE);
			leftColumn = new Function(func.getName(), leftColumn);
			rightColumn = new Function(func.getName(), rightColumn);
		}
		BinaryOperation leftExpr = new BinaryOperation(leftColumn, rightColumn, binaryOperator);
		Expression rightExpr;
		if (Randomly.getBoolean()) {
			rightExpr = new BinaryOperation(leftColumn, rightColumn, binaryOperator.reverse());
		} else {
			rightExpr = new UnaryOperation(UnaryOperator.NOT, leftExpr);
		}
		BinaryOperator conOperator = shouldBeTrue ? BinaryOperator.OR : BinaryOperator.AND;
		return new BinaryOperation(leftExpr, rightExpr, conOperator);
	}

	private Tuple createSampleBasedColumnConstantComparison(Constant sampledConstant, Expression columnName) {
		boolean retry;
		BinaryOperator binaryOperator;
		Expression compareTo;
		SQLite3DataType valueType = sampledConstant.getDataType();

		do {
			binaryOperator = Randomly.fromOptions(BinaryOperator.GREATER_EQUALS, BinaryOperator.SMALLER_EQUALS,
					BinaryOperator.IS, BinaryOperator.NOT_EQUALS, BinaryOperator.GREATER, BinaryOperator.SMALLER,
					BinaryOperator.LIKE, BinaryOperator.EQUALS);
			retry = false;
			switch (binaryOperator) {
			case EQUALS:
				compareTo = Randomly.fromOptions(sampledConstant, columnName);
				break;
			case GREATER_EQUALS:
				if (valueType == SQLite3DataType.INT) {
					compareTo = Randomly.fromOptions(sampledConstant, columnName,
							smallerOrEqualRandomConstant(sampledConstant));
				} else {
					compareTo = Randomly.fromOptions(sampledConstant, columnName);
				}
				break;
			case GREATER:
				compareTo = smallerRandomConstant(sampledConstant);
				if (compareTo == null) {
					retry = true;
				}
				break;
			case IS:
				compareTo = Randomly.fromOptions(sampledConstant, columnName);
				break;
			case LIKE:
				if (valueType == SQLite3DataType.TEXT) {
					StringBuilder sb = new StringBuilder();
					if (Randomly.getBoolean()) {
						sb.append("%");
					}
					String compareToConstant = sampledConstant.asString();
					sb.append(compareToConstant);
					if (Randomly.getBoolean()) {
						sb.append("%");
					}
					compareTo = Constant.createTextConstant(sb.toString());
				} else {
					retry = true;
					compareTo = null;
				}
				break;
			case SMALLER:
				compareTo = greaterRandomConstant(sampledConstant);
				if (compareTo == null) {
					retry = true;
				}
				break;
			case SMALLER_EQUALS:
				if (valueType == SQLite3DataType.INT || valueType == SQLite3DataType.TEXT
						|| valueType == SQLite3DataType.REAL) {
					compareTo = Randomly.fromOptions(sampledConstant, columnName,
							greaterOrEqualRandomConstant(sampledConstant));
				} else {
					compareTo = Randomly.fromOptions(sampledConstant, columnName);
				}
				break;
			case NOT_EQUALS:
				compareTo = notEqualConstant(sampledConstant);
				break;
			default:
				throw new AssertionError(binaryOperator);
			}
		} while (retry);
		return new Tuple(compareTo, binaryOperator);
	}

	private Expression createSampleBasedTwoColumnComparison(List<Column> columns, RowValue rw, Constant sampledConstant,
			Expression columnName, boolean shouldBeTrue) throws AssertionError {
		BinaryOperator operator;
		// relate two columns
		Column otherColumn = Randomly.fromList(columns);
		Constant otherValue = rw.getValues().get(otherColumn);
		SQLite3DataType otherValueType = otherValue.getDataType();
		if (sampledConstant.getDataType() == otherValueType && sampledConstant.getDataType() == SQLite3DataType.INT) {
			long columnValue = sampledConstant.asInt();
			long otherColumnValue = otherValue.asInt();
			if (columnValue > otherColumnValue) {
				operator = Randomly.fromOptions(BinaryOperator.GREATER, BinaryOperator.GREATER_EQUALS,
						BinaryOperator.IS_NOT, BinaryOperator.NOT_EQUALS);
				// !shouldBeTrue and columnValue > otherColumnValue:
			} else if (columnValue < otherColumnValue) {
				operator = Randomly.fromOptions(BinaryOperator.SMALLER, BinaryOperator.SMALLER_EQUALS,
						BinaryOperator.IS_NOT, BinaryOperator.NOT_EQUALS);
			} else {
				operator = Randomly.fromOptions(BinaryOperator.EQUALS, BinaryOperator.IS, BinaryOperator.GREATER_EQUALS,
						BinaryOperator.SMALLER_EQUALS, BinaryOperator.GLOB);
			}
			if (!shouldBeTrue) {
				if (operator == BinaryOperator.GLOB) {
					// FIXME
					return getStandaloneLiteral(shouldBeTrue);
				}
				operator = operator.reverse();
			}
			if (Randomly.getBoolean()) {
				String functionName = getRandomFunction(operator, shouldBeTrue, sampledConstant.getDataType(),
						otherValueType);
				if (functionName != null) {
					Function left = new Expression.Function(functionName, columnName);
					Function right = new Expression.Function(functionName, new Expression.ColumnName(otherColumn));
					return new BinaryOperation(left, right, operator);

				}
			}
			return new BinaryOperation(columnName, new Expression.ColumnName(otherColumn), operator);
		} else if (sampledConstant.getDataType() == otherValueType
				&& sampledConstant.getDataType() == SQLite3DataType.REAL) {
			// duplicated, refactor
			double columnValue = sampledConstant.asDouble();
			double otherColumnValue = otherValue.asDouble();
			if (columnValue > otherColumnValue) {
				operator = Randomly.fromOptions(BinaryOperator.GREATER, BinaryOperator.GREATER_EQUALS,
						BinaryOperator.IS_NOT, BinaryOperator.NOT_EQUALS);
				// !shouldBeTrue and columnValue > otherColumnValue:
			} else if (columnValue < otherColumnValue) {
				operator = Randomly.fromOptions(BinaryOperator.SMALLER, BinaryOperator.SMALLER_EQUALS,
						BinaryOperator.IS_NOT, BinaryOperator.NOT_EQUALS);
			} else {
				operator = Randomly.fromOptions(BinaryOperator.EQUALS, BinaryOperator.IS, BinaryOperator.GREATER_EQUALS,
						BinaryOperator.SMALLER_EQUALS, BinaryOperator.GLOB);
			}
			if (!shouldBeTrue) {
				if (operator == BinaryOperator.GLOB) {
					// FIXME
					return getStandaloneLiteral(shouldBeTrue);
				}
				operator = operator.reverse();
			}
			if (Randomly.getBoolean()) {
				String functionName = getRandomFunction(operator, shouldBeTrue, sampledConstant.getDataType(),
						otherValueType);
				if (functionName != null) {
					Function left = new Expression.Function(functionName, columnName);
					Function right = new Expression.Function(functionName, new Expression.ColumnName(otherColumn));
					return new BinaryOperation(left, right, operator);

				}
			}
			return new BinaryOperation(columnName, new Expression.ColumnName(otherColumn), operator);
		} else {
			// FIXME: should not need this branch
			return getStandaloneLiteral(shouldBeTrue);
		}
	}

	enum BinaryFunction {

		ABS("abs")

		{
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
					return true;
				case IS_NOT:
				case SMALLER:
				case SMALLER_EQUALS:
				case GREATER:
				case GREATER_EQUALS:
				case NOT_EQUALS: // abs(-5) = abs(5)
					return false;
				default:
					throw new AssertionError(operator);
				}
			}

		},
		HEX("hex") {
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
				case IS_NOT:
				case NOT_EQUALS:
					return true;
				case SMALLER:
				case SMALLER_EQUALS:
				case GREATER:
				case GREATER_EQUALS:
					return false;
				default:
					throw new AssertionError(operator);
				}
			}

		},
		UNICODE("unicode") {
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				if (leftType != SQLite3DataType.TEXT || rightType != SQLite3DataType.TEXT) {
					return false;
				}
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
				case IS_NOT:
				case NOT_EQUALS:
				case SMALLER:
				case SMALLER_EQUALS:
				case GREATER:
				case GREATER_EQUALS:
					if (leftType == SQLite3DataType.TEXT || rightType == SQLite3DataType.TEXT) {
						// unicode('') == NULL
						return operator == BinaryOperator.IS_NOT;
					} else {
						return true;
					}
				default:
					throw new AssertionError(operator);
				}
			}
		},
		LTRIM("ltrim") {
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
					return true;
				case NOT_EQUALS:
				case IS_NOT:
					if (leftType != SQLite3DataType.TEXT && rightType != SQLite3DataType.TEXT) {
						return true;
					} else {
						return false;
					}
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					return leftType == SQLite3DataType.TEXT || rightType == SQLite3DataType.TEXT;
				default:
					throw new AssertionError(operator);
				}
			}
		},
		LOWER("lower") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
				case NOT_EQUALS:
				case IS_NOT:
					return true;
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					if (!leftType.isNumeric() && !rightType.isNumeric()) {
						return true;
					} else {
						return false;
					}
				default:
					throw new AssertionError(operator);
				}
			}

		},
		QUOTE("quote") {
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
				case IS_NOT:
				case NOT_EQUALS:
					return true;
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					// does not work due to the e notation
					if (leftType != SQLite3DataType.REAL || rightType != SQLite3DataType.REAL) {
						return true;
					} else {
						return false;
					}
				default:
					throw new AssertionError(operator);
				}
			}

		},
		ROUND("round") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
					return true;
				case IS_NOT:
				case NOT_EQUALS:
				case GREATER:
				case SMALLER:
					if (leftType == SQLite3DataType.REAL && rightType == SQLite3DataType.REAL) {
						return false;
					} else {
						return true;
					}
				default:
					throw new AssertionError(operator);
				}
			}

		},
		RTRIM("rtrim") {
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
					return true;
				case NOT_EQUALS:
				case IS_NOT:
					if (leftType != SQLite3DataType.TEXT && rightType != SQLite3DataType.TEXT) {
						return true;
					} else {
						return false;
					}
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					return leftType == SQLite3DataType.TEXT || rightType == SQLite3DataType.TEXT;
				default:
					throw new AssertionError(operator);
				}
			}
		},
		TRIM("trim") {
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
					return true;
				case NOT_EQUALS:
				case IS_NOT:
					if (leftType != SQLite3DataType.TEXT && rightType != SQLite3DataType.TEXT) {
						return true;
					} else {
						return false;
					}
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					return leftType == SQLite3DataType.TEXT || rightType == SQLite3DataType.TEXT;
				default:
					throw new AssertionError(operator);
				}
			}
		},
		TYPEOF("typeof") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
					return true;
				case NOT_EQUALS:
				case IS_NOT:
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					return false;
				default:
					throw new AssertionError(operator);
				}
			}

		},

		UNLIKELY("unlikely") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				return true;
			}

		},
		UPPER("upper") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
				case NOT_EQUALS:
				case IS_NOT:
					return true;
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					if (!leftType.isNumeric() && !rightType.isNumeric()) {
						return true;
					} else {
						return false;
					}
				default:
					throw new AssertionError(operator);
				}
			}

		},
		LENGTH("length") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
					return true;
				case IS_NOT:
				case NOT_EQUALS:
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
				case GREATER:
				case SMALLER:
					if (!leftType.isNumeric() && !rightType.isNumeric()) {
						return true;
					} else {
						return false;
					}
				default:
					throw new AssertionError(operator);
				}
			}

		},
		LIKELY("likely") {
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				return true;
			}
		};

		private final String name;

		public String getName() {
			return name;
		}

		private BinaryFunction(String name) {
			this.name = name;
		}

		public abstract boolean worksWhenApplied(Expression.BinaryOperation.BinaryOperator operator,
				SQLite3DataType leftType, SQLite3DataType rightType);

		public static List<BinaryFunction> getPossibleBinaryFunctionsForOperator(BinaryOperator operator,
				SQLite3DataType leftDataType, SQLite3DataType rightDataType) {
			return Stream.of(BinaryFunction.values())
					.filter(fun -> fun.worksWhenApplied(operator, leftDataType, rightDataType))
					.collect(Collectors.toList());
		}

	}

	private String getRandomFunction(BinaryOperator operator, boolean shouldBeTrue, SQLite3DataType leftDataType,
			SQLite3DataType rightDataType) {
		if (!shouldBeTrue) {
			operator = operator.reverse();
		}
		List<BinaryFunction> functions = BinaryFunction.getPossibleBinaryFunctionsForOperator(operator, leftDataType,
				rightDataType);

		if (!functions.isEmpty() && Randomly.getBoolean()) {
			return Randomly.fromList(functions).getName();
		}
		return null;
	}

	public static String getRandomUnaryFunction() {
		return Randomly.fromOptions("ABS", "CHAR", "HEX", "LENGTH", "LIKELY", "LOWER", "LTRIM", "QUOTE", "ROUND",
				"RTRIM", "TRIM", "TYPEOF", "UNLIKELY", "UPPER"); // "ZEROBLOB" "UNICODE",
	}

	/**
	 * 
	 * Based on the value of sampledConstant, this method generates an expression
	 * based on a column c so that either "c ISNULL", "C NOT NULL", or "C NOTNULL"
	 * is generated.
	 * 
	 * @param sampledConstant
	 * @param columnName
	 * @return
	 */
	private Expression generateSampleBasedColumnPostfix(Constant sampledConstant, Expression columnName,
			boolean shouldbeTrue) {
		boolean generateIsNull = sampledConstant.isNull() && shouldbeTrue || !sampledConstant.isNull() && !shouldbeTrue;
		if (generateIsNull) {
			return new Expression.PostfixUnaryOperation(PostfixUnaryOperation.PostfixUnaryOperator.ISNULL, columnName);
		} else {
			if (Randomly.getBoolean()) {
				return new Expression.PostfixUnaryOperation(PostfixUnaryOperation.PostfixUnaryOperator.NOT_NULL,
						columnName);
			} else {
				return new Expression.PostfixUnaryOperation(PostfixUnaryOperation.PostfixUnaryOperator.NOTNULL,
						columnName);
			}
		}
	}

	private Constant notEqualConstant(Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			return Constant.createIntConstant(Randomly.notEqualInt(sampledConstant.asInt()));
		case TEXT:
			return Constant.createTextConstant(sampledConstant.asString() + "asdf");
		case REAL:
			return Constant.createRealConstant(sampledConstant.asDouble() % 10 + 0.3);
		case BINARY:
			byte[] asBinary = sampledConstant.asBinary();
			byte[] newBytes = new byte[asBinary.length + 1];
			newBytes[asBinary.length] = 32; // TODO random
			return Constant.createBinaryConstant(newBytes);
		default:
			throw new AssertionError();

		}
	}

	private Constant greaterRandomConstant(Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			if (value == Long.MAX_VALUE) {
				return null;
			} else {
				return Constant.createIntConstant(Randomly.greaterInt(value));
			}
		default:
			return null;
		}
	}

	private Constant smallerRandomConstant(Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			if (value == Long.MIN_VALUE) {
				return null;
			} else {
				return Constant.createIntConstant(Randomly.smallerInt(value));
			}
		case REAL:
			double dValue = sampledConstant.asDouble();
			if (dValue == -Double.MAX_VALUE) {
				return null;
			} else {
				return Constant.createRealConstant(Randomly.smallerDouble(dValue));
			}
		default:
			return null;
		}
	}

	private Constant smallerOrEqualRandomConstant(Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			return Constant.createIntConstant(Randomly.smallerOrEqualInt(value));
		case REAL:
			double dValue = sampledConstant.asDouble();
			return Constant.createRealConstant(Randomly.smallerOrEqualDouble(dValue));
		// TODO: other data types
		default:
			return sampledConstant;
		}
	}

	private Constant greaterOrEqualRandomConstant(Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			return Constant.createIntConstant(Randomly.greaterOrEqualInt(value));
		case TEXT:
			String strValue = sampledConstant.asString();
			return Constant.createTextConstant(Randomly.greaterOrEqualString(strValue));
		case REAL:
			return Constant.createRealConstant(Randomly.greaterOrEqualDouble(sampledConstant.asDouble()));
		default: // TODO: other data types
			return sampledConstant;
		}
	}

}
