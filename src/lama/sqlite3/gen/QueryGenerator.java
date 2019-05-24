package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lama.Main;
import lama.Main.StateLogger;
import lama.Main.StateToReproduce;
import lama.Randomly;
import lama.sqlite3.SQLite3ToStringVisitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.Exist;
import lama.sqlite3.ast.SQLite3Expression.Function;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.Join.JoinType;
import lama.sqlite3.ast.SQLite3Expression.OrderingTerm;
import lama.sqlite3.ast.SQLite3Expression.OrderingTerm.Ordering;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral.Type;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation.UnaryOperator;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.RowValue;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class QueryGenerator {

	private final Connection database;
	private final SQLite3Schema s;
	private final Randomly r;
	private StateToReproduce state;
	private RowValue rw;
	private List<Column> fetchColumns;

	public QueryGenerator(Connection con, Randomly r) throws SQLException {
		this.database = con;
		this.r = r;
		s = SQLite3Schema.fromConnection(database);
	}

	public void generateAndCheckQuery(StateToReproduce state, StateLogger logger) throws SQLException {
		String queryString = getQueryThatContainsAtLeastOneRow(state);
		// logger.writeCurrent(queryString);

		boolean isContainedIn = isContainedIn(queryString);
		if (!isContainedIn) {
			throw new Main.ReduceMeException();
		}
	}

	public String getQueryThatContainsAtLeastOneRow(StateToReproduce state) throws SQLException {
		this.state = state;
		Tables randomFromTables = s.getRandomTableNonEmptyTables();
		List<Table> tables = randomFromTables.getTables();

		state.queryTargetedTablesString = randomFromTables.tableNamesAsString();
		SQLite3SelectStatement selectStatement = new SQLite3SelectStatement();
		selectStatement.setSelectType(Randomly.fromOptions(SQLite3SelectStatement.SelectType.values()));
		List<Column> columns = randomFromTables.getColumns();
		for (Table t : tables) {
			if (t.getRowid() != null) {
				columns.add(t.getRowid());
			}
		}
		rw = randomFromTables.getRandomRowValue(database, state);

		List<Join> joinStatements = new ArrayList<>();
		for (int i = 1; i < tables.size(); i++) {
			SQLite3Expression joinClause = generateWhereClauseThatContainsRowValue(columns, rw);
			Table table = Randomly.fromList(tables);
			tables.remove(table);
			JoinType options;
			if (tables.size() == 2) {
				// allow outer with arbitrary column order (see error: ON clause references
				// tables to its right)
				options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER);
			} else {
				options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS);
			}
			Join j = new SQLite3Expression.Join(table, joinClause, options);
			joinStatements.add(j);
		}
		selectStatement.setJoinClauses(joinStatements);
		selectStatement.setFromTables(tables);

		// TODO: also implement a wild-card check (*)
		// filter out row ids from the select because the hinder the reduction process
		// once a bug is found
		List<Column> columnsWithoutRowid = columns.stream().filter(c -> !c.getName().matches("rowid"))
				.collect(Collectors.toList());
		fetchColumns = Randomly.nonEmptySubset(columnsWithoutRowid);
		selectStatement.selectFetchColumns(fetchColumns);
		state.queryTargetedColumnsString = fetchColumns.stream().map(c -> c.getFullQualifiedName())
				.collect(Collectors.joining(", "));
		SQLite3Expression whereClause = generateWhereClauseThatContainsRowValue(columns, rw);
		selectStatement.setWhereClause(whereClause);
		state.whereClause = selectStatement;
		List<SQLite3Expression> groupByClause = generateGroupByClause(columns, rw);
		selectStatement.setGroupByClause(groupByClause);
		SQLite3Expression limitClause = generateLimit();
		selectStatement.setLimitClause(limitClause);
		if (limitClause != null) {
			SQLite3Expression offsetClause = generateOffset();
			selectStatement.setOffsetClause(offsetClause);
		}
		List<SQLite3Expression> orderBy = generateOrderBy(columns);
		selectStatement.setOrderByClause(orderBy);
		SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
		visitor.visit(selectStatement);
		String queryString = visitor.get();
		return queryString;
	}

	private SQLite3Expression generateOffset() {
		if (Randomly.getBoolean()) {
			// OFFSET 0
			return SQLite3Constant.createIntConstant(0);
		} else {
			return null;
		}
	}

	private boolean isContainedIn(String queryString) throws SQLException {
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
		state.queryString = resultingQueryString;
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

	public static boolean shouldIgnoreException(SQLException e) {
		return e.getMessage().contentEquals("[SQLITE_ERROR] SQL error or missing database (integer overflow)")
				|| e.getMessage().startsWith("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
	}

	public List<SQLite3Expression> generateOrderBy(List<Column> columns) {
		List<SQLite3Expression> orderBys = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			SQLite3Expression expr;
			expr = SQLite3Constant.createTextConstant(r.getString());
			Ordering order = Randomly.fromOptions(Ordering.ASC, Ordering.DESC);
			orderBys.add(new OrderingTerm(expr, order));
			// TODO RANDOM()
		}
		// TODO collate
		return orderBys;
	}

	private SQLite3Expression generateLimit() {
		if (Randomly.getBoolean()) {
			return SQLite3Constant.createIntConstant(Integer.MAX_VALUE);
		} else {
			return null;
		}
	}

	private List<SQLite3Expression> generateGroupByClause(List<Column> columns, RowValue rw) {
		if (Randomly.getBoolean()) {
			return columns.stream().map(c -> new ColumnName(c, rw.getValues().get(c))).collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	private SQLite3Expression generateWhereClauseThatContainsRowValue(List<Column> columns, RowValue rw) {

		SQLite3Expression whereClause = generateNewExpression(columns, rw, true, 0);

		return whereClause;
	}

	private enum NewExpressionType {
		CAST_TO_ITSELF, LITERAL, STANDALONE_COLUMN, DOUBLE_COLUMN, POSTFIX_COLUMN, NOT, UNARY_PLUS, AND, OR, COLLATE,
		UNARY_FUNCTION, IN, ALWAYS_TRUE_COLUMN_COMPARISON, CAST_TO_NUMERIC, SEVERAL_DOUBLE_COLUMN,
		LEFT_RIGHT_SIDE_EQUALS, MORGAN_LAW, KNOWN_EXPRESSION
	}

	private SQLite3Expression generateNewExpression(List<Column> columns, RowValue rw, boolean shouldBeTrue,
			int depth) {
		if (depth >= Main.EXPRESSION_MAX_DEPTH) {
			return getStandaloneLiteral(shouldBeTrue);
		}
		if (Randomly.getBoolean()) {
			int nr = 5;
			SQLite3Expression exp = null;
			for (int i = 0; i < nr; i++) {
				SQLite3Expression comp = createSampleBasedTwoColumnComparison(columns, rw, shouldBeTrue);
				if (exp == null) {
					exp = comp;
				} else {
					exp = new BinaryOperation(exp, comp,
							Randomly.getBoolean() ? BinaryOperator.OR : BinaryOperator.AND);
				}
			}
		}
		if (Randomly.getBoolean() && shouldBeTrue) {
			return generateExpression(columns, rw);
		}
		boolean retry;
		do {
			retry = false;
			SQLite3Constant sampledConstant;
			switch (Randomly.fromOptions(NewExpressionType.values())) {
			case KNOWN_EXPRESSION:
				SQLite3Expression expr = new SQLite3ExpressionGenerator(rw).getRandomExpression(columns, false, r);
				if (expr.getExpectedValue() != null) {
					if (shouldBeTrue) {
						return new BinaryOperation(expr.getExpectedValue(), expr, BinaryOperator.IS);
					} else {
						return new BinaryOperation(expr.getExpectedValue(), expr, BinaryOperator.IS_NOT);

					}
//					if (isTrue(expr.getExpectedValue())) {
//						return expr;
//					} else {
//						return new BinaryOperation(expr.getExpectedValue(), expr, BinaryOperator.IS);
//					}
				}
			case MORGAN_LAW:
				return createMorgansLawClause(columns, rw, shouldBeTrue, depth);
			case LEFT_RIGHT_SIDE_EQUALS:
				SQLite3Expression subExpr1 = generateNewExpression(columns, rw, Randomly.getBoolean(), depth + 1);
				if (shouldBeTrue) {
					return new BinaryOperation(subExpr1, subExpr1, BinaryOperator.IS);
				} else {
					return new BinaryOperation(subExpr1, subExpr1, BinaryOperator.IS_NOT);
				}
			case CAST_TO_NUMERIC:
				SQLite3Expression subExpr2 = createStandaloneColumn(columns, rw, true);
				if (subExpr2 == null) {
					retry = true;
					break;
				} else {
					SQLite3Constant value = rw.getValues().get(((ColumnName) subExpr2).getColumn());
					assert !value.isNull();
					TypeLiteral typeofExpr = new SQLite3Expression.TypeLiteral(Type.NUMERIC);
					Cast castExpr = new SQLite3Expression.Cast(typeofExpr, subExpr2);
					SQLite3Constant castConstant = SQLite3Cast.castToNumeric(value);
					assert castConstant != null;
					return new BinaryOperation(castExpr, castConstant,
							shouldBeTrue ? BinaryOperator.EQUALS : BinaryOperator.NOT_EQUALS);
				}
			case ALWAYS_TRUE_COLUMN_COMPARISON:
				SQLite3Expression alwaysTrue = createAlwaysTrueColumnComparison(columns, rw, shouldBeTrue);
				if (alwaysTrue == null) {
					retry = true;
					continue;
				} else {
					return alwaysTrue;
				}
			case CAST_TO_ITSELF: // TODO: let each expression have a type so that CAST can be applied without
									// typeof
				SQLite3Expression subExpr = createStandaloneColumn(columns, rw, shouldBeTrue);
				if (subExpr == null) {
					retry = true;
					break;
				}
				ColumnName column = (ColumnName) subExpr;
				assert rw.getValues().get(column.getColumn()) != null;
				SQLite3Expression.TypeLiteral.Type type;
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
				TypeLiteral typeofExpr = new SQLite3Expression.TypeLiteral(type);
				return new SQLite3Expression.Cast(typeofExpr, subExpr);
			case STANDALONE_COLUMN:
				expr = createStandaloneColumn(columns, rw, shouldBeTrue);
				if (expr == null) {
					retry = true;
					continue;
				} else {
					if (Randomly.getBoolean()) {
						// TODO combine two standalone columns like this
						expr = new BinaryOperation(expr, expr, BinaryOperator.MULTIPLY);
					}
					return expr;
				}
			case SEVERAL_DOUBLE_COLUMN:
				int nr = Randomly.smallNumber() + 1;
				SQLite3Expression exp = null;
				for (int i = 0; i < nr; i++) {
					SQLite3Expression comp = createSampleBasedTwoColumnComparison(columns, rw, shouldBeTrue);
					if (exp == null) {
						exp = comp;
					} else {
						exp = new BinaryOperation(exp, comp,
								Randomly.getBoolean() ? BinaryOperator.OR : BinaryOperator.AND);
					}
				}
				return exp;
			case DOUBLE_COLUMN:
				return createSampleBasedTwoColumnComparison(columns, rw, shouldBeTrue);
			case POSTFIX_COLUMN:
				Column c = Randomly.fromList(columns);
				sampledConstant = rw.getValues().get(c);
				return generateSampleBasedColumnPostfix(sampledConstant, new ColumnName(c, sampledConstant), shouldBeTrue);
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
				return new SQLite3Expression.CollateOperation(
						generateNewExpression(columns, rw, shouldBeTrue, depth + 1),
						Randomly.fromOptions("NOCASE", "RTRIM", "BINARY"));
			case AND:
				SQLite3Expression left;
				SQLite3Expression right;
				left = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
				if (shouldBeTrue) {
					right = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
					return new SQLite3Expression.BinaryOperation(left, right, BinaryOperator.AND);
				} else {
					right = generateNewExpression(columns, rw, Randomly.getBoolean(), depth + 1);
					return new SQLite3Expression.BinaryOperation(left, right, BinaryOperator.AND);
				}
			case OR:
				SQLite3Expression leftExpr = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
				SQLite3Expression rightExpr;
				if (shouldBeTrue) {
					// one side can be false
					if (Randomly.getBoolean()) {
						rightExpr = generateNewExpression(columns, rw, Randomly.getBoolean(), depth + 1);
					} else {
						rightExpr = new SQLite3ExpressionGenerator(rw).getRandomExpression(columns, false, r);
					}

					if (Randomly.getBoolean()) {
						// swap to allow leftExpr to be false
						SQLite3Expression tmpExpression = leftExpr;
						leftExpr = rightExpr;
						rightExpr = tmpExpression;
					}
					return new SQLite3Expression.BinaryOperation(leftExpr, rightExpr, BinaryOperator.OR);
				} else {
					rightExpr = generateNewExpression(columns, rw, shouldBeTrue, depth + 1);
					return new SQLite3Expression.BinaryOperation(leftExpr, rightExpr, BinaryOperator.OR);
				}
			case IN:
				c = Randomly.fromList(columns);
				List<SQLite3Expression> expressions = new ArrayList<>();
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
								expressions.add(
										new SQLite3ExpressionGenerator(rw).getRandomExpression(columns, depth + 1, false, r));
							}
							int randomPosition = r.getInteger(0, expressions.size());
							if (Randomly.getBoolean()) {
								expressions.add(randomPosition, new SQLite3Expression.ColumnName(c, rw.getValues().get(c)));
							} else {
								expressions.add(randomPosition, rw.getValues().get(c));
							}
						} else {
							String query = "SELECT " + c.getName() + " FROM " + c.getTable().getName();
							expressions.add(SQLite3Expression.Subquery.create(query));
						}
					} else {
						for (int i = 0; i < Randomly.smallNumber(); i++) {
							expressions.add(notEqualConstant(rw.getValues().get(c)));
							// TODO also not equals column
						}
					}
				}
				return new SQLite3Expression.InOperation(new SQLite3Expression.ColumnName(c, rw.getValues().get(c)), expressions);
			default:
				throw new AssertionError();
			}
		} while (retry);
		throw new AssertionError();
	}

	private SQLite3Expression createMorgansLawClause(List<Column> columns, RowValue rw, boolean shouldBeTrue,
			int depth) {
		SQLite3Expression l = generateNewExpression(columns, rw, Randomly.getBoolean(), depth + 1);
		SQLite3Expression r = generateNewExpression(columns, rw, Randomly.getBoolean(), depth + 1);
		SQLite3Expression ll;
		SQLite3Expression rr;
		if (Randomly.getBoolean()) {
			ll = new UnaryOperation(UnaryOperation.UnaryOperator.NOT, new BinaryOperation(l, r, BinaryOperator.OR)); // not
																														// (l
																														// or
																														// r1)
			rr = new BinaryOperation(new UnaryOperation(UnaryOperation.UnaryOperator.NOT, l),
					new UnaryOperation(UnaryOperation.UnaryOperator.NOT, r), BinaryOperation.BinaryOperator.AND); // not
																													// l
																													// and
																													// not
																													// r1

		} else {
			ll = new UnaryOperation(UnaryOperation.UnaryOperator.NOT, new BinaryOperation(l, r, BinaryOperator.AND));
			rr = new BinaryOperation(new UnaryOperation(UnaryOperator.NOT, l), new UnaryOperation(UnaryOperator.NOT, r),
					BinaryOperator.OR);
		}
		if (shouldBeTrue) {
			return new BinaryOperation(ll, rr, BinaryOperator.IS);
		} else {
			return new BinaryOperation(ll, rr, BinaryOperator.IS_NOT);
		}
	}

	private SQLite3Expression createAlwaysTrueColumnComparison(List<Column> columns, RowValue rw,
			boolean shouldBeTrue) {
		Column left = Randomly.fromList(columns);
		Column right = Randomly.fromList(columns);
		return createAlwaysTrueTwoColumnExpression(left, right, rw, shouldBeTrue);
	}

	private SQLite3Expression getUnaryFunction() {
		String functionName = Randomly.fromOptions("sqlite_source_id", "sqlite_version",
				"total_changes" /* some rows should have been inserted */);
		return new SQLite3Expression.Function(functionName);
	}

	
	
	// e.g., WHERE c0 or
	private SQLite3Expression createStandaloneColumn(List<Column> columns, RowValue rw, boolean shouldBeTrue) {
		Column c = Randomly.fromList(columns);
		SQLite3Constant value = rw.getValues().get(c);
		if (value.isNull()) {
			return null;
		}
		SQLite3Constant numericValue;
		if (value.getDataType() == SQLite3DataType.TEXT || value.getDataType() == SQLite3DataType.BINARY) {
			numericValue = SQLite3Cast.castToNumeric(value);
		} else {
			numericValue = value;
		}
		assert numericValue.getDataType() != SQLite3DataType.TEXT : numericValue + "should have been converted";
		switch (numericValue.getDataType()) {
		case INT:
			if (shouldBeTrue && numericValue.asInt() != 0) {
				return new ColumnName(c, value);
			} else if (!shouldBeTrue && numericValue.asInt() == 0) {
				return new ColumnName(c, value);
			} else {
				return null;
			}
		case REAL:
			// directly comparing to a double is probably not a good idea
			if (shouldBeTrue && numericValue.asDouble() != 0.0) {
				return new ColumnName(c, value);
			} else if (!shouldBeTrue && numericValue.asDouble() == 0.0) {
				return new ColumnName(c, value);
			} else {
				return null;
			}
		default:
			throw new AssertionError(numericValue);
		}
	}

	private SQLite3Expression getStandaloneLiteral(boolean shouldBeTrue) throws AssertionError {
		switch (Randomly.fromOptions(SQLite3DataType.INT, SQLite3DataType.TEXT, SQLite3DataType.REAL)) {
		case INT:
			// only a zero integer is false
			long value;
			if (shouldBeTrue) {
				value = r.getNonZeroInteger();
			} else {
				value = 0;
			}
			return SQLite3Constant.createIntConstant(value);
		case TEXT:
			String strValue;
			if (shouldBeTrue) {
				strValue = r.getNonZeroString();
			} else {
				strValue = Randomly.fromOptions("0", "asdf", "c", "-a");
			}
			return SQLite3Constant.createTextConstant(strValue);
		case REAL:
			double realValue;
			if (shouldBeTrue) {
				realValue = r.getNonZeroReal();
			} else {
				realValue = Randomly.fromOptions(0.0, -0.0);
			}
			return SQLite3Constant.createRealConstant(realValue);
		default:
			throw new AssertionError();
		}
	}

	private SQLite3Expression generateExpression(List<Column> columns, RowValue rw) throws AssertionError {
		SQLite3Expression term;

		if (Randomly.getBoolean()) {
			term = generateOpaquePredicate(true);
		} else {
			// select column
			Column selectedColumn = Randomly.fromList(columns);
			SQLite3Constant sampledConstant = rw.getValues().get(selectedColumn);

			SQLite3Expression compareTo;
			BinaryOperator binaryOperator;
			SQLite3DataType valueType = sampledConstant.getDataType();

			SQLite3Expression columnName = new SQLite3Expression.ColumnName(selectedColumn, sampledConstant);
			if (Randomly.getBoolean()) {
				term = generateSampleBasedColumnPostfix(sampledConstant, columnName, true);
			} else {
				if (sampledConstant.isNull()) {
					// is null, comparison with >= etc. yields false
					binaryOperator = BinaryOperator.IS;
					compareTo = Randomly.fromOptions(sampledConstant, columnName);
				} else if (Randomly.getBoolean()) {
					return createSampleBasedTwoColumnComparison(columns, rw, true);
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
						columnName = new SQLite3Expression.Function(function, columnName);
						compareTo = new SQLite3Expression.Function(function, compareTo);
					}
				}
				term = new SQLite3Expression.BinaryOperation(columnName, compareTo, binaryOperator);
			}
		}
		return term;
	}

	private SQLite3Expression generateOpaquePredicate(boolean shouldBeTrue) {
		BinaryOperator operator = Randomly.fromOptions(BinaryOperator.EQUALS, BinaryOperator.GREATER_EQUALS,
				BinaryOperator.IS, BinaryOperator.SMALLER_EQUALS);
		SQLite3Expression term;
		// generate opaque predicate
		SQLite3Expression con;
		SQLite3DataType randomType = Randomly.fromOptions(SQLite3DataType.INT, SQLite3DataType.TEXT,
				SQLite3DataType.NULL);

		if (Randomly.getBoolean()) {
			SQLite3SelectStatement randomQuery = SQLite3RandomQueryGenerator.generateRandomQuery(s, r);
			Exist exist = new Exist(randomQuery);
			UnaryOperation notExist = new UnaryOperation(UnaryOperator.NOT, exist);
			term = new SQLite3Expression.BinaryOperation(exist, notExist, BinaryOperator.OR);
		}
		switch (randomType) {
		// TODO REAL
		case INT:
			long val = r.getInteger();
			con = SQLite3Constant.createIntConstant(val);
			break;
		case TEXT:
			con = SQLite3Constant.createTextConstant(r.getString());
			break;
		case BINARY:
			byte[] bytes = new byte[Randomly.smallNumber()];
			Randomly.getBytes(bytes);
			con = SQLite3Constant.createBinaryConstant(bytes);
			break;
		case NULL:
			con = SQLite3Constant.createNullConstant();
			operator = BinaryOperator.IS;
			break;
		default:
			throw new AssertionError();
		}

		if (Randomly.getBoolean()) {
			// apply function
			con = new SQLite3Expression.Function(getRandomUnaryFunction(), con);
		}
		term = new SQLite3Expression.BinaryOperation(con, con, operator);
		if (shouldBeTrue) {
			return term;
		} else {
			return new UnaryOperation(UnaryOperation.UnaryOperator.NOT, term);
		}
	}

	class Tuple {
		public Tuple(SQLite3Expression compareTo, BinaryOperator binaryOperator) {
			this.op = binaryOperator;
			this.expr = compareTo;
		}

		BinaryOperator op;
		SQLite3Expression expr;
	}

	private SQLite3Expression createAlwaysTrueTwoColumnExpression(Column left, Column right, RowValue rw,
			boolean shouldBeTrue) {
		BinaryOperator binaryOperator = Randomly.fromOptions(BinaryOperator.GREATER_EQUALS,
				BinaryOperator.SMALLER_EQUALS, BinaryOperator.IS, BinaryOperator.NOT_EQUALS, BinaryOperator.GREATER,
				BinaryOperator.SMALLER, BinaryOperator.EQUALS);
		SQLite3Expression leftColumn = new ColumnName(left, rw.getValues().get(left));
		SQLite3Expression rightColumn = new ColumnName(right, rw.getValues().get(right));

		boolean leftIsNull = rw.getValues().get(left).isNull();
		boolean rightIsNull = rw.getValues().get(right).isNull();

		// generate the left hand expression, for example, a < b
		BinaryOperation leftExpr = new BinaryOperation(leftColumn, rightColumn, binaryOperator);

		SQLite3Expression rightExpr;
		// for the right hand side, reverse the expression, for example, a >= b
		if (Randomly.getBoolean()) {
			rightExpr = new BinaryOperation(leftColumn, rightColumn, binaryOperator.reverse());
		} else {
			rightExpr = new UnaryOperation(UnaryOperator.NOT, leftExpr);
		}

		BinaryOperator conOperator = shouldBeTrue ? BinaryOperator.OR : BinaryOperator.AND;
		BinaryOperation binOp = new BinaryOperation(leftExpr, rightExpr, conOperator);
		if ((leftIsNull || rightIsNull) && binaryOperator != BinaryOperator.IS
				&& binaryOperator != BinaryOperator.IS_NOT) {
			if (shouldBeTrue) {
				return new PostfixUnaryOperation(PostfixUnaryOperator.ISNULL, binOp);
			} else {
				return new PostfixUnaryOperation(PostfixUnaryOperator.NOT_NULL, binOp);
			}
		}
		return binOp;
	}

	private Tuple createSampleBasedColumnConstantComparison(SQLite3Constant sampledConstant,
			SQLite3Expression columnName) {
		boolean retry;
		BinaryOperator binaryOperator;
		SQLite3Expression compareTo;
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
				compareTo = Randomly.fromOptions(sampledConstant, columnName,
						smallerOrEqualRandomConstant(sampledConstant));
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
					compareTo = SQLite3Constant.createTextConstant(sb.toString());
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
				compareTo = Randomly.fromOptions(sampledConstant, columnName,
						greaterOrEqualRandomConstant(sampledConstant));
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

	/**
	 * Selects two random columns and relates them via a comparison operation.
	 * 
	 * @param columns
	 * @param rw
	 * @param shouldBeTrue
	 * @return
	 * @throws AssertionError
	 */
	private SQLite3Expression createSampleBasedTwoColumnComparison(List<Column> columns, RowValue rw,
			boolean shouldBeTrue) throws AssertionError {

		Column leftColumn = Randomly.fromList(columns);
		SQLite3Constant leftColumnValue = rw.getValues().get(leftColumn);
		SQLite3Expression leftColumnExpr = new SQLite3Expression.ColumnName(leftColumn, leftColumnValue);
		SQLite3DataType originalLeftColumnType = leftColumnValue.getDataType();

		Column rightColumn = Randomly.fromList(columns);
		SQLite3Constant rightColumnValue = rw.getValues().get(rightColumn);
		SQLite3Expression rightColumnExpr = new SQLite3Expression.ColumnName(rightColumn, rightColumnValue);
		SQLite3DataType originalRightColumnType = rightColumnValue.getDataType();

		// If one operand has INTEGER, REAL or NUMERIC affinity and the other operand
		// has TEXT or BLOB or no affinity then NUMERIC affinity is applied to other
		// operand.
		if (leftColumn.getColumnType().isNumeric() && (rightColumn.getColumnType() == SQLite3DataType.TEXT
				|| rightColumn.getColumnType() == SQLite3DataType.BINARY
				|| rightColumn.getColumnType() == SQLite3DataType.NONE)) {
			rightColumnValue = rightColumnValue.applyNumericAffinity();
		} else if (rightColumn.getColumnType().isNumeric()
				&& (leftColumn.getColumnType() == SQLite3DataType.TEXT
						|| leftColumn.getColumnType() == SQLite3DataType.BINARY)
				|| leftColumn.getColumnType() == SQLite3DataType.NONE) {
			leftColumnValue = leftColumnValue.applyNumericAffinity();
		}

		// If one operand has TEXT affinity and the other has no affinity, then TEXT
		// affinity is applied to the other operand.
		if (leftColumn.getColumnType() == SQLite3DataType.TEXT && rightColumn.getColumnType() == SQLite3DataType.NONE) {
			rightColumnValue = rightColumnValue.applyTextAffinity();
		} else if (rightColumn.getColumnType() == SQLite3DataType.TEXT
				&& leftColumn.getColumnType() == SQLite3DataType.NONE) {
			leftColumnValue = leftColumnValue.applyTextAffinity();
		}

		List<BinaryOperator> newOp = leftColumnValue.compare(rightColumnValue, shouldBeTrue);
		if (newOp.isEmpty()) {
			// TODO
			return getStandaloneLiteral(shouldBeTrue);
		}
		BinaryOperator operator = Randomly.fromList(newOp);
		// Randomly.getBoolean() ? rightColumnExpr : rightColumnValue
		if (Randomly.getBoolean()) {
			String functionName = getRandomFunction(operator, shouldBeTrue, originalLeftColumnType,
					originalRightColumnType);
			if (functionName != null) {
				Function left = new SQLite3Expression.Function(functionName, leftColumnExpr);
				Function right = new SQLite3Expression.Function(functionName,
						new SQLite3Expression.ColumnName(rightColumn, rightColumnValue));
				new BinaryOperation(left, right, operator);
				// TODO
			}
		}
		return new BinaryOperation(leftColumnExpr, rightColumnExpr, operator);

	}

	enum BinaryFunction {

		ABS("abs")

		{
			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				// SELECT ABS(x'F4');
				if (leftType != rightType) {
					return false;
				}
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
				if (leftType != rightType) {
					return false;
				}
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
				return false;
//				switch (operator) {
//				case IS:
//				case EQUALS:
//				case GLOB:
//				case LIKE:
//				case IS_NOT:
//				case NOT_EQUALS:
//				case SMALLER:
//				case SMALLER_EQUALS:
//				case GREATER:
//				case GREATER_EQUALS:
//					if (leftType == SQLite3DataType.TEXT || rightType == SQLite3DataType.TEXT) {
//						// unicode('') == NULL
//						return operator == BinaryOperator.IS_NOT;
//					} else {
//						return true;
//					}
//				default:
//					throw new AssertionError(operator);
//				}
			}
		},
		LTRIM("ltrim") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				if (leftType != rightType) {
					return false;
				}
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
				if (leftType != rightType) {
					return false;
				}
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
				return false;
//				switch (operator) {
//				case IS:
//				case EQUALS:
//				case GLOB:
//				case LIKE:
//				case IS_NOT:
//				case NOT_EQUALS:
//					return true;
//				case SMALLER_EQUALS:
//				case GREATER_EQUALS:
//				case GREATER:
//				case SMALLER:
//					// does not work due to the e notation
//					if (leftType != SQLite3DataType.REAL || rightType != SQLite3DataType.REAL) {
//						return true;
//					} else {
//						return false;
//					}
//				default:
//					throw new AssertionError(operator);
//				}
			}

		},
		ROUND("round") {

			@Override
			public boolean worksWhenApplied(BinaryOperator operator, SQLite3DataType leftType,
					SQLite3DataType rightType) {
				if (leftType != rightType) {
					return false;
				}
				switch (operator) {
				case IS:
				case EQUALS:
				case GLOB:
				case LIKE:
				case SMALLER_EQUALS:
				case GREATER_EQUALS:
					if (leftType == SQLite3DataType.REAL || rightType == SQLite3DataType.REAL) {
						return false;
					} else {
						return true;
					}
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
				if (leftType != rightType) {
					return false;
				}
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
				if (leftType != rightType) {
					return false;
				}
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
				if (leftType != rightType) {
					return false;
				}
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

		public abstract boolean worksWhenApplied(SQLite3Expression.BinaryOperation.BinaryOperator operator,
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
		if (leftDataType == SQLite3DataType.BINARY || rightDataType == SQLite3DataType.BINARY) {
			return null;
		}
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
	private SQLite3Expression generateSampleBasedColumnPostfix(SQLite3Constant sampledConstant,
			SQLite3Expression columnName, boolean shouldbeTrue) {
		boolean generateIsNull = sampledConstant.isNull() && shouldbeTrue || !sampledConstant.isNull() && !shouldbeTrue;
		if (generateIsNull) {
			return new SQLite3Expression.PostfixUnaryOperation(PostfixUnaryOperation.PostfixUnaryOperator.ISNULL,
					columnName);
		} else {
			if (Randomly.getBoolean()) {
				return new SQLite3Expression.PostfixUnaryOperation(PostfixUnaryOperation.PostfixUnaryOperator.NOT_NULL,
						columnName);
			} else {
				return new SQLite3Expression.PostfixUnaryOperation(PostfixUnaryOperation.PostfixUnaryOperator.NOTNULL,
						columnName);
			}
		}
	}

	private SQLite3Constant notEqualConstant(SQLite3Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			return SQLite3Constant.createIntConstant(r.notEqualInt(sampledConstant.asInt()));
		case TEXT:
			return SQLite3Constant.createTextConstant(sampledConstant.asString() + "asdf");
		case REAL:
			return SQLite3Constant.createRealConstant(sampledConstant.asDouble() % 10 + 0.3);
		case BINARY:
			byte[] asBinary = sampledConstant.asBinary();
			byte[] newBytes = new byte[asBinary.length + 1];
			newBytes[asBinary.length] = 32; // TODO random
			return SQLite3Constant.createBinaryConstant(newBytes);
		default:
			throw new AssertionError();

		}
	}

	private SQLite3Constant greaterRandomConstant(SQLite3Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			if (value == Long.MAX_VALUE) {
				return null;
			} else {
				return SQLite3Constant.createIntConstant(r.greaterInt(value));
			}
		case REAL:
			double dValue = sampledConstant.asDouble();
			if (dValue == Double.POSITIVE_INFINITY) {
				return null;
			} else {
				return SQLite3Constant.createRealConstant(r.greaterDouble(dValue));
			}
		default:
			return null;
		}
	}

	private SQLite3Constant smallerRandomConstant(SQLite3Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			if (value == Long.MIN_VALUE) {
				return null;
			} else {
				return SQLite3Constant.createIntConstant(r.smallerInt(value));
			}
		case REAL:
			double dValue = sampledConstant.asDouble();
			if (dValue == Double.NEGATIVE_INFINITY) {
				return null;
			} else {
				return SQLite3Constant.createRealConstant(r.smallerDouble(dValue));
			}
		default:
			return null;
		}
	}

	private SQLite3Constant smallerOrEqualRandomConstant(SQLite3Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			return SQLite3Constant.createIntConstant(r.smallerOrEqualInt(value));
		case REAL:
			double dValue = sampledConstant.asDouble();
			return SQLite3Constant.createRealConstant(r.smallerOrEqualDouble(dValue));
		// TODO: other data types
		default:
			return sampledConstant;
		}
	}

	private SQLite3Constant greaterOrEqualRandomConstant(SQLite3Constant sampledConstant) {
		switch (sampledConstant.getDataType()) {
		case INT:
			long value = sampledConstant.asInt();
			return SQLite3Constant.createIntConstant(r.greaterOrEqualInt(value));
		case TEXT:
			String strValue = sampledConstant.asString();
			return SQLite3Constant.createTextConstant(r.greaterOrEqualString(strValue));
		case REAL:
			return SQLite3Constant.createRealConstant(r.greaterOrEqualDouble(sampledConstant.asDouble()));
		default: // TODO: other data types
			return sampledConstant;
		}
	}

}
