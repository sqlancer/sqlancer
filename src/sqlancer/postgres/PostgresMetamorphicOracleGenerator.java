package sqlancer.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.StateToReproduce.PostgresStateToReproduce;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.ast.PostgresPostfixText;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.SelectType;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresMetamorphicOracleGenerator {

	private PostgresSchema s;
	private Randomly r;
	private Connection con;
	private PostgresStateToReproduce state;
	private String firstQueryString;
	private String secondQueryString;
	private StateLogger logger;
	private MainOptions options;
	private final List<String> errors = new ArrayList<>();
	private PostgresGlobalState globalState;

	public PostgresMetamorphicOracleGenerator(PostgresSchema s, Randomly r, Connection con,
			PostgresStateToReproduce state, StateLogger logger, MainOptions options, QueryManager manager, PostgresGlobalState globalState) {
		this.s = s;
		this.r = r;
		this.con = con;
		this.state = state;
		this.logger = logger;
		this.options = options;
		this.globalState = globalState;
	}

	public void generateAndCheck() throws SQLException {
		PostgresCommon.addCommonExpressionErrors(errors);
		PostgresCommon.addCommonFetchErrors(errors);
		PostgresTables randomTables = s.getRandomTableNonEmptyTables();
		List<PostgresColumn> columns = randomTables.getColumns();
		PostgresExpression randomWhereCondition = getRandomWhereCondition(columns);
		List<PostgresExpression> groupBys;
		if (Randomly.getBooleanWithSmallProbability()) {
			groupBys = getRandomExpressions(columns);
		} else {
			groupBys = Collections.emptyList();
		}
		List<PostgresTable> tables = randomTables.getTables();

		List<PostgresJoin> joinStatements = new ArrayList<>();
		for (int i = 1; i < tables.size(); i++) {
			PostgresExpression joinClause = getRandomWhereCondition(columns);
			PostgresTable table = Randomly.fromList(tables);
			tables.remove(table);
			PostgresJoinType options = PostgresJoinType.getRandom();
			PostgresJoin j = new PostgresJoin(table, joinClause, options);
			joinStatements.add(j);
		}
		List<PostgresFromTable> fromTables = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
				.collect(Collectors.toList());
		int secondCount = getSecondQuery(fromTables, randomWhereCondition, groupBys, joinStatements);
		int firstCount = getFirstQueryCount(con, fromTables, columns, randomWhereCondition, groupBys, joinStatements);
		if (firstCount == -1 || secondCount == -1) {
			throw new IgnoreMeException();
		}
		if (firstCount != secondCount) {
			state.queryString = firstCount + " " + secondCount + " " + firstQueryString + ";\n" + secondQueryString + ";";
			throw new AssertionError(firstQueryString + secondQueryString + firstCount + " " + secondCount);
		}
	}

	private List<PostgresExpression> getRandomExpressions(List<PostgresColumn> columns) {
		List<PostgresExpression> randomExpressions = columns.stream().map(c -> new PostgresColumnValue(c, null))
				.collect(Collectors.toList());
		if (Randomly.getBoolean()) {
			for (int i = 0; i < Randomly.smallNumber(); i++) {
				randomExpressions.add(getRandomWhereCondition(columns));
			}
		}
		return randomExpressions;
	}

	private PostgresExpression getRandomWhereCondition(List<PostgresColumn> columns) {
		return new PostgresExpressionGenerator(r).setColumns(columns).setGlobalState(globalState).generateExpression(PostgresDataType.BOOLEAN);
	}

	private int getSecondQuery(List<PostgresFromTable> fromTables, PostgresExpression randomWhereCondition,
			List<PostgresExpression> groupBys, List<PostgresJoin> joinStatements) throws SQLException {
		PostgresSelect select = new PostgresSelect();
//		select.setGroupByClause(groupBys);
//		PostgresExpression isTrue = PostgresPostfixOperation.create(randomWhereCondition, PostfixOperator.IS_TRUE);
		PostgresCastOperation isTrue = new PostgresCastOperation(randomWhereCondition, PostgresCompoundDataType.create(PostgresDataType.INT));
		PostgresPostfixText asText = new PostgresPostfixText(isTrue, " as count", null, PostgresDataType.INT);
		select.setFetchColumns(Arrays.asList(asText));
		select.setFromTables(fromTables);
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		int secondCount = 0;
		secondQueryString = "SELECT SUM(count) FROM (" + PostgresVisitor.asString(select) + ") as res";
		if (options.logEachSelect()) {
			logger.writeCurrent(secondQueryString);
		}
		errors.add("canceling statement due to statement timeout");
		Query q = new QueryAdapter(secondQueryString, errors);
		ResultSet rs;
		try {
			rs = q.executeAndGet(con);
		} catch (Exception e) {
			throw new AssertionError(secondQueryString, e);
		}
		if (rs == null) {
			return -1;
		}
		if (rs.next()) {
			secondCount += rs.getLong(1);
		}
		rs.close();
		return secondCount;
	}

	private int getFirstQueryCount(Connection con, List<PostgresFromTable> randomTables, List<PostgresColumn> columns,
			PostgresExpression randomWhereCondition, List<PostgresExpression> groupBys,
			List<PostgresJoin> joinStatements) throws SQLException {
		PostgresSelect select = new PostgresSelect();
//		select.setGroupByClause(groupBys);
//		PostgresAggregate aggr = new PostgresAggregate(
				PostgresColumnValue allColumns = new PostgresColumnValue(Randomly.fromList(columns), null);
//				PostgresAggregateFunction.COUNT);
//		select.setFetchColumns(Arrays.asList(aggr));
				select.setFetchColumns(Arrays.asList(allColumns));
		select.setFromTables(randomTables);
		select.setWhereClause(randomWhereCondition);
		if (Randomly.getBooleanWithSmallProbability()) {
			select.setOrderByClause(new PostgresExpressionGenerator(r).setColumns(columns).setGlobalState(globalState).generateOrderBy());
		}
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		int firstCount = 0;
		try (Statement stat = con.createStatement()) {
			firstQueryString = PostgresVisitor.asString(select);
			if (options.logEachSelect()) {
				logger.writeCurrent(firstQueryString);
			}
			try (ResultSet rs = stat.executeQuery(firstQueryString)) {
				while (rs.next()) {
					firstCount++;
				}
			}
		} catch (SQLException e) {
			throw new IgnoreMeException();
		}
		return firstCount;
	}
	
//	private int getFirstAlternativeQueryCount(Connection con, List<PostgresFromTable> randomTables, List<PostgresColumn> columns,
//			PostgresExpression randomWhereCondition, List<PostgresExpression> groupBys,
//			List<PostgresJoin> joinStatements) throws SQLException {
//		PostgresSelect select = new PostgresSelect();
////		select.setGroupByClause(groupBys);
//		PostgresColumnValue aggr = new PostgresColumnValue(new PostgresColumn("*", PostgresDataType.INT), null);
//		select.setFetchColumns(Arrays.asList(aggr));
//		select.setFromTables(randomTables);
//		select.setWhereClause(randomWhereCondition);
//		if (Randomly.getBooleanWithSmallProbability()) {
////			select.setOrderByClause(getRandomExpressions(columns));
//		}
//		select.setSelectType(SelectType.ALL);
//		select.setJoinClauses(joinStatements);
//		int firstCount = 0;
//		try (Statement stat = con.createStatement()) {
//			firstQueryString = PostgresVisitor.asString(select);
////			if (options.logEachSelect()) {
////				logger.writeCurrent(firstQueryString);
////			}
//			try (ResultSet rs = stat.executeQuery(firstQueryString)) {
//				while (rs.next()) {
//					firstCount += 1;
//				}
//			}
//		} catch (SQLException e) {
//			throw new IgnoreMeException();
//		}
//		return firstCount;
//	}

}
