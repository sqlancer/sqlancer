package lama.sqlite3.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lama.Randomly;
import lama.sqlite3.SQLite3Provider;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.ast.SQLite3Aggregate;
import lama.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import lama.sqlite3.ast.SQLite3Case.CasePair;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithBaseExpression;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithoutBaseExpression;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BetweenOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;
import lama.sqlite3.ast.SQLite3Expression.CollateOperation;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.MatchOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Exist;
import lama.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import lama.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm.Ordering;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Text;
import lama.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation.BinaryOperator;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Function;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;
import lama.sqlite3.ast.SQLite3RowValue;
import lama.sqlite3.ast.SQLite3UnaryOperation;
import lama.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import lama.sqlite3.queries.SQLite3RandomQuerySynthesizer;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;
import lama.sqlite3.schema.SQLite3Schema.RowValue;

public class SQLite3ExpressionGenerator {

	private RowValue rw;
	private final SQLite3GlobalState globalState;
	private boolean tryToGenerateKnownResult;
	private List<Column> columns = Collections.emptyList();
	private final Randomly r;
	private boolean deterministicOnly;
	private boolean allowMatchClause;
	private boolean allowAggregateFunctions;

	private enum LiteralValueType {
		INTEGER, NUMERIC, STRING, BLOB_LITERAL, NULL
	}

	public SQLite3ExpressionGenerator(SQLite3GlobalState globalState) {
		this.globalState = globalState;
		this.r = globalState.getRandomly();
	}

	public SQLite3ExpressionGenerator deterministicOnly() {
		this.deterministicOnly = true;
		return this;
	}

	public SQLite3ExpressionGenerator allowAggregateFunctions() {
		this.allowAggregateFunctions = true;
		return this;
	}

	public SQLite3ExpressionGenerator setColumns(List<Column> columns) {
		this.columns = columns;
		return this;
	}

	public SQLite3ExpressionGenerator setRowValue(RowValue rw) {
		this.rw = rw;
		return this;
	}

	public SQLite3ExpressionGenerator allowMatchClause() {
		this.allowMatchClause = true;
		return this;
	}

	public SQLite3ExpressionGenerator tryToGenerateKnownResult() {
		this.tryToGenerateKnownResult = true;
		return this;
	}

	public static SQLite3Expression getRandomLiteralValue(SQLite3GlobalState globalState) {
		return new SQLite3ExpressionGenerator(globalState).getRandomLiteralValueInternal(globalState.getRandomly());
	}

	public SQLite3Expression generateOrderingTerm(Randomly r) {
		SQLite3Expression expr = getRandomExpression();
		// COLLATE is potentially already generated
		if (Randomly.getBoolean()) {
			expr = new SQLite3OrderingTerm(expr, Ordering.getRandomValue());
		}
		if (Randomly.getBoolean()) {
			expr = new SQLite3PostfixText(expr, Randomly.fromOptions(" NULLS FIRST", " NULLS LAST"),
					expr.getExpectedValue());
		}
		return expr;
	}

	/***
	 * 
	 * @see https://www.sqlite.org/syntax/literal-value.html
	 * @return
	 */
	private SQLite3Expression getRandomLiteralValueInternal(Randomly r) {
		LiteralValueType randomLiteral = Randomly.fromOptions(LiteralValueType.values());
		switch (randomLiteral) {
		case INTEGER:
			return SQLite3Constant.createIntConstant(r.getInteger());
		case NUMERIC:
			return SQLite3Constant.createRealConstant(r.getDouble());
		case STRING:
			return SQLite3Constant.createTextConstant(r.getString());
		case BLOB_LITERAL:
			return SQLite3Constant.getRandomBinaryConstant(r);
		case NULL:
			return SQLite3Constant.createNullConstant();
		default:
			throw new AssertionError(randomLiteral);
		}
	}

	enum ExpressionType {
		/* RANDOM_QUERY, */ COLUMN_NAME, LITERAL_VALUE, UNARY_OPERATOR, POSTFIX_UNARY_OPERATOR, BINARY_OPERATOR,
		BETWEEN_OPERATOR, CAST_EXPRESSION, BINARY_COMPARISON_OPERATOR, FUNCTION, IN_OPERATOR, COLLATE, EXISTS,
		CASE_OPERATOR, MATCH, AGGREGATE_FUNCTION, ROW_VALUE_COMPARISON
	}

	public SQLite3Expression getRandomExpression() {
		return getRandomExpression(0);
	}

	public List<SQLite3Expression> getRandomExpressions(int size) {
		List<SQLite3Expression> expressions = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			expressions.add(getRandomExpression());
		}
		return expressions;
	}

	public List<SQLite3Expression> getRandomExpressions(int size, int depth) {
		List<SQLite3Expression> expressions = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			expressions.add(getRandomExpression(depth));
		}
		return expressions;
	}

	public SQLite3Expression getRandomExpression(int depth) {
		if (depth >= SQLite3Provider.EXPRESSION_MAX_DEPTH) {
			if (Randomly.getBoolean() || columns.isEmpty()) {
				return getRandomLiteralValue(globalState);
			} else {
				return getRandomColumn();
			}
		}

		List<ExpressionType> list = new ArrayList<>(Arrays.asList(ExpressionType.values()));
		if (columns.isEmpty()) {
			list.remove(ExpressionType.COLUMN_NAME);
		}
		if (!allowMatchClause) {
			list.remove(ExpressionType.MATCH);
		}
		if (!allowAggregateFunctions) {
			list.remove(ExpressionType.AGGREGATE_FUNCTION);
		}
//		if (con == null) {
//			list.remove(ExpressionType.RANDOM_QUERY);
//		}
		ExpressionType randomExpressionType = Randomly.fromList(list);
		switch (randomExpressionType) {
		case LITERAL_VALUE:
			return getRandomLiteralValue(globalState);
		case COLUMN_NAME:
			return getRandomColumn();
		case UNARY_OPERATOR:
			return getRandomUnaryOperator(depth + 1);
		case POSTFIX_UNARY_OPERATOR:
			return getRandomPostfixUnaryOperator(depth + 1);
		case BINARY_OPERATOR:
			return getBinaryOperator(depth + 1);
		case BINARY_COMPARISON_OPERATOR:
			return getBinaryComparisonOperator(depth + 1);
		case BETWEEN_OPERATOR:
			return getBetweenOperator(depth + 1);
		case CAST_EXPRESSION:
			return getCastOperator(depth + 1);
		case FUNCTION:
			return getFunction(depth);
		case IN_OPERATOR:
			return getInOperator(depth + 1);
		case COLLATE:
			return new CollateOperation(getRandomExpression(depth + 1), CollateSequence.random());
		case EXISTS:
			return getExists();
		case CASE_OPERATOR:
			return getCaseOperator(depth + 1);
		case MATCH:
			return getMatchClause(depth);
		case AGGREGATE_FUNCTION:
			return new SQLite3Aggregate(getRandomExpression(depth + 1), SQLite3AggregateFunction.getRandom());
		case ROW_VALUE_COMPARISON:
			return getRowValueComparison(depth + 1);
//		case RANDOM_QUERY:
//			// TODO: pass schema from the outside
//			// TODO: depth
//			try {
//				return SQLite3RandomQuerySynthesizer.generate(SQLite3Schema.fromConnection(con), r, 1);
//			} catch (SQLException e) {
//				throw new AssertionError(e);
//			}
		default:
			throw new AssertionError(randomExpressionType);
		}
	}

	private enum RowValueComparison {
		STANDARD_COMPARISON, BETWEEN /* , IN */
	}

	private SQLite3Expression getRowValueComparison(int depth) {
		int size = Randomly.smallNumber() + 1;
		RowValueComparison randomOption = Randomly.fromOptions(RowValueComparison.values());
		List<SQLite3Expression> left = getRandomExpressions(size, depth + 1);
		List<SQLite3Expression> right = getRandomExpressions(size, depth + 1);
		switch (randomOption) {
		case STANDARD_COMPARISON:
			return new BinaryComparisonOperation(new SQLite3RowValue(left), new SQLite3RowValue(right),
					BinaryComparisonOperator.getRandomRowValueOperator());
		case BETWEEN:
			return new BetweenOperation(getRandomRowValue(depth + 1, size), Randomly.getBoolean(),
					new SQLite3RowValue(left), new SQLite3RowValue(right));
		// TODO: IN operator (right hand side must be a query)
		default:
			throw new AssertionError(randomOption);
		}
	}

	private SQLite3RowValue getRandomRowValue(int depth, int size) {
		return new SQLite3RowValue(getRandomExpressions(size, depth + 1));
	}

	private SQLite3Expression getMatchClause(int depth) {
		SQLite3Expression left = getRandomExpression(depth + 1);
		SQLite3Expression right;
		if (Randomly.getBoolean()) {
			right = getRandomExpression(depth + 1);
		} else {
			right = SQLite3TextConstant.createTextConstant(SQLite3MatchStringGenerator.generateMatchString(r));
		}
		return new MatchOperation(left, right);
	}

	private SQLite3Expression getExists() {
		SQLite3Expression val;
		if (tryToGenerateKnownResult
				|| (!Randomly.getBooleanWithSmallProbability() || globalState.getConnection() == null || globalState.getState() == null || globalState == null)) {
			if (Randomly.getBoolean()) {
				val = new SQLite3Text("SELECT 1", SQLite3Constant.createIntConstant(1));
			} else {
				val = new SQLite3Text("SELECT 0 WHERE 0", SQLite3Constant.createIntConstant(0));
			}
		} else {
			// generating a random query is costly, so do it infrequently
			val = SQLite3RandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1);
		}
		return new SQLite3Exist(val);
	}

	private SQLite3Expression getRandomColumn() {
		Column c = Randomly.fromList(columns);
		return new ColumnName(c, rw == null ? null : rw.getValues().get(c));
	}

	enum Attribute {
		VARIADIC, NONDETERMINISTIC
	};

	private enum AnyFunction {
		ABS("ABS", 1), //
		CHANGES("CHANGES", 0, Attribute.NONDETERMINISTIC), //
		CHAR("CHAR", 1, Attribute.VARIADIC), //
		COALESCE("COALESCE", 2, Attribute.VARIADIC), //
		GLOB("GLOB", 2), //
		HEX("HEX", 1), //
		IFNULL("IFNULL", 2), //
		INSTR("INSTR", 2), //
		LAST_INSERT_ROWID("LAST_INSERT_ROWID", 0, Attribute.NONDETERMINISTIC), //
		LENGTH("LENGTH", 1), //
		LIKE("LIKE", 2), //
		LIKE2("LIKE", 3) {
			@Override
			List<SQLite3Expression> generateArguments(int nrArgs, int depth, SQLite3ExpressionGenerator gen) {
				List<SQLite3Expression> args = super.generateArguments(nrArgs, depth, gen);
				args.set(2, gen.getRandomSingleCharString());
				return args;
			}
		}, //
		LIKELIHOOD("LIKELIHOOD", 2), //
		LIKELY("LIKELY", 1), //
		LOAD_EXTENSION("load_extension", 1), //
		LOAD_EXTENSION2("load_extension", 2, Attribute.NONDETERMINISTIC), LOWER("LOWER", 1), //
		LTRIM1("LTRIM", 1), //
		LTRIM2("LTRIM", 2), //
		MAX("MAX", 2, Attribute.VARIADIC), //
		MIN("MIN", 2, Attribute.VARIADIC), //
		NULLIF("NULLIF", 2), //
		PRINTF("PRINTF", 1, Attribute.VARIADIC), //
		QUOTE("QUOTE", 1), //
		ROUND("ROUND", 2), //
		RTRIM("RTRIM", 1), //
		SOUNDEX("soundex", 1), //
		SQLITE_COMPILEOPTION_GET("SQLITE_COMPILEOPTION_GET", 1, Attribute.NONDETERMINISTIC), //
		SQLITE_COMPILEOPTION_USED("SQLITE_COMPILEOPTION_USED", 1, Attribute.NONDETERMINISTIC), //
//		SQLITE_OFFSET(1), //
		SQLITE_SOURCE_ID("SQLITE_SOURCE_ID", 0, Attribute.NONDETERMINISTIC),
		SQLITE_VERSION("SQLITE_VERSION", 0, Attribute.NONDETERMINISTIC), //
		SUBSTR("SUBSTR", 2), //
		TOTAL_CHANGES("TOTAL_CHANGES", 0, Attribute.NONDETERMINISTIC), //
		TRIM("TRIM", 1), //
		TYPEOF("TYPEOF", 1), //
		UNICODE("UNICODE", 1), UNLIKELY("UNLIKELY", 1), //
		UPPER("UPPER", 1), // "ZEROBLOB"
//	   ZEROBLOB("ZEROBLOB", 1),
		DATE("DATE", 3, Attribute.VARIADIC), //
		TIME("TIME", 3, Attribute.VARIADIC), //
		DATETIME("DATETIME", 3, Attribute.VARIADIC), //
		JULIANDAY("JULIANDAY", 3, Attribute.VARIADIC), STRFTIME("STRFTIME", 3, Attribute.VARIADIC),
		// json functions
		JSON("json", 1), //
		JSON_ARRAY("json_array", 2, Attribute.VARIADIC), // no such function: json_array for blob
															// values
		JSON_ARRAY_LENGTH("json_array_length", 1), //
		JSON_TYPE("json_type", 1), //
		JSON_VALID("json_valid", 1), JSON_QUOTE("json_quote", 1);

		private int minNrArgs;
		private boolean variadic;
		private boolean deterministic;
		private String name;

		private AnyFunction(String name, int minNrArgs, Attribute... attributes) {
			this.name = name;
			List<Attribute> attrs = Arrays.asList(attributes);
			this.minNrArgs = minNrArgs;
			this.variadic = attrs.contains(Attribute.VARIADIC);
			this.deterministic = !attrs.contains(Attribute.NONDETERMINISTIC);
		}

		public boolean isVariadic() {
			return variadic;
		}

		public int getMinNrArgs() {
			return minNrArgs;
		}

		static AnyFunction getRandom() {
			return Randomly.fromOptions(AnyFunction.values());
		}

		static AnyFunction getRandomDeterministic() {
			return Randomly.fromList(
					Stream.of(AnyFunction.values()).filter(f -> f.deterministic).collect(Collectors.toList()));
		}

		@Override
		public String toString() {
			return name;
		}

		List<SQLite3Expression> generateArguments(int nrArgs, int depth, SQLite3ExpressionGenerator gen) {
			List<SQLite3Expression> expressions = new ArrayList<>();
			for (int i = 0; i < nrArgs; i++) {
				expressions.add(gen.getRandomExpression(depth + 1));
			}
			return expressions;
		}
	}

	private SQLite3Expression getFunction(int depth) {
		if (tryToGenerateKnownResult || Randomly.getBoolean()) {
			return getComputableFunction(depth + 1);
		} else {
			AnyFunction randomFunction;
			if (deterministicOnly) {
				randomFunction = AnyFunction.getRandomDeterministic();
			} else {
				randomFunction = AnyFunction.getRandom();
			}
			int nrArgs = randomFunction.getMinNrArgs();
			if (randomFunction.isVariadic()) {
				nrArgs += Randomly.smallNumber();
			}
			List<SQLite3Expression> expressions = randomFunction.generateArguments(nrArgs, depth + 1, this);
			return new SQLite3Expression.Function(randomFunction.toString(),
					expressions.toArray(new SQLite3Expression[0]));
		}

	}

	protected SQLite3Expression getRandomSingleCharString() {
		String s;
		do {
			s = r.getString();
		} while (s.isEmpty());
		return new SQLite3TextConstant(String.valueOf(s.charAt(0)));
	}

	private SQLite3Expression getCaseOperator(int depth) {
		int nrCaseExpressions = 1 + Randomly.smallNumber();
		CasePair[] pairs = new CasePair[nrCaseExpressions];
		for (int i = 0; i < pairs.length; i++) {
			SQLite3Expression whenExpr = getRandomExpression(depth + 1);
			SQLite3Expression thenExpr = getRandomExpression(depth + 1);
			CasePair pair = new CasePair(whenExpr, thenExpr);
			pairs[i] = pair;
		}
		SQLite3Expression elseExpr;
		if (Randomly.getBoolean()) {
			elseExpr = getRandomExpression(depth + 1);
		} else {
			elseExpr = null;
		}
		if (Randomly.getBoolean()) {
			return new SQLite3CaseWithoutBaseExpression(pairs, elseExpr);
		} else {
			SQLite3Expression baseExpr = getRandomExpression(depth + 1);
			return new SQLite3CaseWithBaseExpression(baseExpr, pairs, elseExpr);
		}
	}

	private SQLite3Expression getCastOperator(int depth) {
		SQLite3Expression expr = getRandomExpression(depth + 1);
		TypeLiteral type = new SQLite3Expression.TypeLiteral(
				Randomly.fromOptions(SQLite3Expression.TypeLiteral.Type.values()));
		return new SQLite3Expression.Cast(type, expr);
	}

	private SQLite3Expression getComputableFunction(int depth) {
		ComputableFunction func = ComputableFunction.getRandomFunction();
		int nrArgs = func.getNrArgs();
		if (func.isVariadic()) {
			nrArgs += Randomly.smallNumber();
		}
		SQLite3Expression[] args = new SQLite3Expression[nrArgs];
		for (int i = 0; i < args.length; i++) {
			args[i] = getRandomExpression(depth + 1);
			if (i == 0 && Randomly.getBoolean()) {
				args[i] = new SQLite3Distinct(args[i]);
			}
		}
		SQLite3Function sqlFunction = new SQLite3Function(func, args);
		return sqlFunction;
	}

	private SQLite3Expression getBetweenOperator(int depth) {
		boolean tr = Randomly.getBoolean();
		SQLite3Expression expr = getRandomExpression(depth + 1);
		SQLite3Expression left = getRandomExpression(depth + 1);
		SQLite3Expression right = getRandomExpression(depth + 1);
		return new SQLite3Expression.BetweenOperation(expr, tr, left, right);
	}

	// TODO: incomplete
	private SQLite3Expression getBinaryOperator(int depth) {
		SQLite3Expression leftExpression = getRandomExpression(depth + 1);
		// TODO: operators
		BinaryOperator operator = BinaryOperator.getRandomOperator();
//		while (operator == BinaryOperator.DIVIDE) {
//			operator = BinaryOperator.getRandomOperator();
//		}
		SQLite3Expression rightExpression = getRandomExpression(depth + 1);
		return new SQLite3Expression.Sqlite3BinaryOperation(leftExpression, rightExpression, operator);
	}

	private SQLite3Expression getInOperator(int depth) {
		SQLite3Expression leftExpression = getRandomExpression(depth + 1);
		List<SQLite3Expression> right = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			right.add(getRandomExpression(depth + 1));
		}
		return new SQLite3Expression.InOperation(leftExpression, right);
	}

	private SQLite3Expression getBinaryComparisonOperator(int depth) {
		SQLite3Expression leftExpression = getRandomExpression(depth + 1);
		BinaryComparisonOperator operator = BinaryComparisonOperator.getRandomOperator();
		SQLite3Expression rightExpression = getRandomExpression(depth + 1);
		return new SQLite3Expression.BinaryComparisonOperation(leftExpression, rightExpression, operator);
	}

	// complete
	private SQLite3Expression getRandomPostfixUnaryOperator(int depth) {
		SQLite3Expression subExpression = getRandomExpression(depth + 1);
		PostfixUnaryOperator operator = PostfixUnaryOperator.getRandomOperator();
		return new SQLite3Expression.SQLite3PostfixUnaryOperation(operator, subExpression);
	}

	// complete
	public SQLite3Expression getRandomUnaryOperator(int depth) {
		SQLite3Expression subExpression = getRandomExpression(depth + 1);
		UnaryOperator unaryOperation = Randomly.fromOptions(UnaryOperator.values());
		return new SQLite3UnaryOperation(unaryOperation, subExpression);
	}

}
