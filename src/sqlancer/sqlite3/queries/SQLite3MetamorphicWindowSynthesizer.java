package sqlancer.sqlite3.queries;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Cast;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Function;
import sqlancer.sqlite3.ast.SQLite3Function.ComputableFunction;
import sqlancer.sqlite3.ast.SQLite3SelectStatement;
import sqlancer.sqlite3.ast.SQLite3SelectStatement.SelectType;
import sqlancer.sqlite3.ast.SQLite3WindowFunction;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3FrameSpecKind;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecBetween;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecTerm;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecTerm.SQLite3WindowFunctionFrameSpecTermKind;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3RowValue;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3MetamorphicWindowSynthesizer implements TestOracle {

	private final SQLite3GlobalState state;
	private List<SQLite3Expression> partitionBy;
	private Map<String, Integer> columnNameToIndex;
	List<String> errors = new ArrayList<>();
	private String firstQueryString;
	private String secondQueryString;
	private SQLite3RowValue pivotRow;
	private PivotRowResult unoptimizedWindowFunctionResult;
	private PivotRowResult optimizedWindowFunctionResult;
	private List<SQLite3Column> rowIds;

	public SQLite3MetamorphicWindowSynthesizer(SQLite3GlobalState state) {
		this.state = state;
		errors.add("second argument to nth_value must be a positive integer");
		SQLite3Errors.addExpectedExpressionErrors(errors);
	}

	public AssertionError throwError() {
		StringBuilder sb = new StringBuilder();
		if (firstQueryString != null) {
			sb.append("optimized query: ");
			sb.append(firstQueryString);
		}
		if (optimizedWindowFunctionResult != null) {
			sb.append("\nactual window function result: ");
			sb.append(optimizedWindowFunctionResult);
		}
		if (secondQueryString != null) {
			sb.append("\nunoptimized query: ");
			sb.append(secondQueryString);
		}
		if (unoptimizedWindowFunctionResult != null) {
			sb.append("\nunoptimized window function result: ");
			sb.append(unoptimizedWindowFunctionResult);
		}
		if (pivotRow != null) {
			sb.append("\npivot row: ");
			sb.append(pivotRow);
		}
		throw new AssertionError(sb.toString());
	}

	@Override
	public void check() throws SQLException {

		partitionBy = null;
		columnNameToIndex = null;
		firstQueryString = null;
		pivotRow = null;
		unoptimizedWindowFunctionResult = null;
		optimizedWindowFunctionResult = null;
		SQLite3Schema s = state.getSchema();
		SQLite3Tables targetTables = s.getRandomTableNonEmptyTables();
		if (targetTables.getTables().size() != 1) {
			throw new IgnoreMeException(); // TODO
		}
//		for (Table tab : targetTables.getTables()) {
//			if (tab.getRowid() == null) {
//				throw new IgnoreMeException();
//			} else {
//				rowIds.add(tab.getRowid());
//			}
//		}
		// step 1: generate window function
		SQLite3SelectStatement select = generateWindowSelect(s, targetTables);
		// step 2: generate pivot row
		pivotRow = targetTables.getRandomRowValue(state.getConnection(), state.getState());
		// step 3: identify partition by value (to do a manual group by)
		PivotRowResult result = getResultRow(select, pivotRow);
		optimizedWindowFunctionResult = result;
		// step 4: compute window
		SQLite3SelectStatement metamorphicSelect = new SQLite3SelectStatement();
		metamorphicSelect.setFetchColumns(select.getFetchColumns());
		SQLite3WindowFunctionExpression originalWindowFunction = (SQLite3WindowFunctionExpression) metamorphicSelect
				.getFetchColumns().get(WINDOW_FUNCTION_RESULT_OFFSET);
		metamorphicSelect.setOrderByClause(originalWindowFunction.getOrderBy());
		originalWindowFunction.setFrameSpec(null);
		originalWindowFunction.setPartitionBy(Collections.emptyList());
		originalWindowFunction.setFrameSpec(null);
		metamorphicSelect.setWhereClause(originalWindowFunction.getFilterClause());
		originalWindowFunction.setFilterClause(null);
		// step 5: adapt window
		
		metamorphicSelect.setWhereClause(
				new BinaryComparisonOperation(result.groupByValue, partitionBy.get(0), BinaryComparisonOperator.IS));
		metamorphicSelect.setFromTables(select.getFromList());
//		metamorphicSelect.setLimitClause(SQLite3Constant.createIntConstant(1));
		secondQueryString = SQLite3Visitor.asString(metamorphicSelect);
		QueryAdapter q = new QueryAdapter(secondQueryString, errors);
		try {
			state.getLogger().getCurrentFileWriter().write(firstQueryString + "\n");
			state.getLogger().getCurrentFileWriter().write(secondQueryString + ";\n");
			state.getLogger().getCurrentFileWriter().flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try (ResultSet rs = q.executeAndGet(state.getConnection())) {
			unoptimizedWindowFunctionResult = identifyPivotRowResult(pivotRow, rs);

			if (rs == null) {
				throw new IgnoreMeException(); // TODO: check
			}
			if (!rs.next()) {
				throw new IgnoreMeException(); // TODO: check
			}
//			System.out.println(result.windowFunctionValue);
//			System.out.println(windowFunctionResult);
			state.getState().queryString = "--" + pivotRow + "\n" + firstQueryString + "\n" + secondQueryString
					+ "\n";
			state.getState().randomRowValues = pivotRow.getValues();
			if ((result.windowFunctionValue == null && unoptimizedWindowFunctionResult.windowFunctionValue != null)
					|| result.windowFunctionValue != null && !result.windowFunctionValue
							.contentEquals(unoptimizedWindowFunctionResult.windowFunctionValue)) {
				throwError();
			}
			rs.getStatement().close();
		} catch (SQLException e) {
			// TODO check
		}
		// stpe 5: compute queries
//			System.out.println(pivotRow);

	}

	static class PivotRowResult {
		String windowFunctionValue;
		SQLite3Constant groupByValue;
		Map<SQLite3Column, String> valueMap = new HashMap<>();
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("-- window function: " + windowFunctionValue);
			sb.append("-- partition by: " + groupByValue);
			sb.append(valueMap);
			return sb.toString();
		}
	}

	private PivotRowResult getResultRow(SQLite3SelectStatement select, SQLite3RowValue pivotRow) throws SQLException {
		firstQueryString = SQLite3Visitor.asString(select);
		if (firstQueryString.contains("EXISTS")) {
			throw new IgnoreMeException(); // TODO
		}
//		if (firstQueryString.contains("LEAD") || firstQueryString.contains("LAST_VALUE") || firstQueryString.contains("LAG") || firstQueryString.contains("FIRST_VALUE") || firstQueryString.contains("NTH_VALUE") || firstQueryString.contains("PERCENT_RANK") || firstQueryString.contains("GROUP_CONCAT")) {
//			throw new IgnoreMeException();
//		}
		QueryAdapter q = new QueryAdapter(firstQueryString, errors);
		try (ResultSet result = q.executeAndGet(state.getConnection())) {
			PivotRowResult identifyPivotRowResult = identifyPivotRowResult(pivotRow, result);
			result.getStatement().close();
			return identifyPivotRowResult;
		} catch (Exception e) {
			if (e instanceof IgnoreMeException) {
				throw e;
			}
			throw new AssertionError(firstQueryString, e);
		}
	}

	private final static int PARTITION_BY_OFFSET = 0;
	private final static int PARTITION_BY_TYPE_OFFSET = 1;
	private final static int WINDOW_FUNCTION_RESULT_OFFSET = 2;

	private PivotRowResult identifyPivotRowResult(SQLite3RowValue pivotRow, ResultSet result) {
		if (result == null) {
			throw new IgnoreMeException();
		}
		try {
			while (result.next()) {
				Map<SQLite3Column, String> valueMap = new HashMap<>();
				for (SQLite3Column c : pivotRow.getValues().keySet()) {
					int correspondingIndex = columnNameToIndex.get(c.getName());
					String columnValue = result.getString(correspondingIndex);
					SQLite3Constant sqLite3Constant = pivotRow.getValues().get(c);
					if (sqLite3Constant.isNull()) {
						if (columnValue != null) {
							continue;
						}
					} else {
						SQLite3Constant constant = SQLite3Schema.getConstant(result, correspondingIndex, sqLite3Constant.getDataType());
						SQLite3Constant res = sqLite3Constant.applyEquals(constant);
						if (sqLite3Constant.isNull() && !constant.isNull() || !sqLite3Constant.isNull() && constant.isNull() || !res.isNull() && SQLite3Cast.asBoolean(res).asInt() != 1) {
							continue;
						}
					}

					valueMap.put(c, columnValue);
				}
				PivotRowResult r = new PivotRowResult();
				r.windowFunctionValue = result.getString(WINDOW_FUNCTION_RESULT_OFFSET + 1);
				String columnString = result.getString(PARTITION_BY_TYPE_OFFSET + 1);
				SQLite3DataType partitionByType = SQLite3Schema.getColumnType(columnString);
				if (partitionByType == SQLite3DataType.BINARY || partitionByType == SQLite3DataType.TEXT) {
					throw new IgnoreMeException(); // TODO: why does this not work?
				}
				r.groupByValue = SQLite3Schema.getConstant(result, PARTITION_BY_OFFSET + 1, partitionByType);
				r.valueMap = valueMap;
				return r;
			}
			result.close();
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		throw throwError();
	}

	// parition by, partition by type, window function, row values
	private SQLite3SelectStatement generateWindowSelect(SQLite3Schema s, SQLite3Tables targetTables) {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		// DISTINCT or ALL
		SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(state).setColumns(targetTables.getColumns());
		select.setFromOptions(SelectType.ALL);
		List<SQLite3Column> columns = targetTables.getColumns();
		SQLite3Expression baseWindowFunction;
		if (Randomly.getBoolean()) {
			baseWindowFunction = gen.getAggregateFunction(true);
		} else {
			baseWindowFunction = SQLite3WindowFunction.getRandom(targetTables.getColumns(), state);
		}
		SQLite3WindowFunction.getRandom(columns, state);
		SQLite3WindowFunctionExpression windowFunction = new SQLite3WindowFunctionExpression(baseWindowFunction);
		partitionBy = gen.getRandomExpressions(1);
		windowFunction.setPartitionBy(partitionBy); // TODO: extend to multiple partition bys
		if (Randomly.getBoolean() && false) {
			windowFunction.setFilterClause(gen.getRandomExpression());
		}
		windowFunction.setFrameSpecKind(SQLite3FrameSpecKind.ROWS);
		SQLite3WindowFunctionFrameSpecTerm left = new SQLite3WindowFunctionFrameSpecTerm(
				SQLite3WindowFunctionFrameSpecTermKind.UNBOUNDED_PRECEDING);
		SQLite3WindowFunctionFrameSpecTerm right = new SQLite3WindowFunctionFrameSpecTerm(
				SQLite3WindowFunctionFrameSpecTermKind.UNBOUNDED_FOLLOWING);
		windowFunction.setFrameSpec(new SQLite3WindowFunctionFrameSpecBetween(left, right));
		if (Randomly.getBoolean() || true) {
			// TODO ORDER BYs are only necessary for window functions where the order is
			// important (e.g., LEAD)
			windowFunction.setOrderBy(targetTables.getColumns().stream().map(c -> new SQLite3ColumnName(c, null)).collect(Collectors.toList()));
		}
		ArrayList<SQLite3Expression> fetchList = new ArrayList<SQLite3Expression>();
		fetchList.addAll(partitionBy);
		fetchList.add(new SQLite3Function(ComputableFunction.TYPEOF, partitionBy.get(0)));
		fetchList.add(windowFunction);
		columnNameToIndex = new HashMap<>();
		for (int i = 0; i < columns.size(); i++) {
			fetchList.add(new SQLite3ColumnName(columns.get(i), null));
			columnNameToIndex.put(columns.get(i).getName(), i + WINDOW_FUNCTION_RESULT_OFFSET + 1);
		}
		select.setFetchColumns(fetchList);
		select.setFromList(SQLite3Common.getTableRefs(targetTables.getTables(), s));
		return select;
	}

	@Override
	public boolean onlyWorksForNonEmptyTables() {
		return true;
	}

}
