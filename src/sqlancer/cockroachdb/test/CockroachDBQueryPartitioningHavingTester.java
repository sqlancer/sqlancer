package sqlancer.cockroachdb.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import sqlancer.IgnoreMeException;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;

public class CockroachDBQueryPartitioningHavingTester implements TestOracle {
	
	private final CockroachDBGlobalState state;
	private final Set<String> errors = new HashSet<>();

	public CockroachDBQueryPartitioningHavingTester(CockroachDBGlobalState state) {
		this.state = state;
		CockroachDBErrors.addExpressionErrors(errors);
	}

	@Override
	public void check() throws SQLException {
		CockroachDBSchema s = state.getSchema();
		CockroachDBTables targetTables = s.getRandomTableNonEmptyTables();
		CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
		CockroachDBSelect select = new CockroachDBSelect();
		select.setColumns(Arrays.asList(new CockroachDBColumnReference(targetTables.getColumns().get(0))));
		List<CockroachDBTableReference> tableList = targetTables.getTables().stream()
				.map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
		List<CockroachDBTableReference> from = CockroachDBCommon.getTableReferences(tableList);
		if (Randomly.getBooleanWithRatherLowProbability()) {
			select.setJoinList(CockroachDBNoRECTester.getJoins(from, state));
		}
		select.setFromTables(from);
		if (Randomly.getBooleanWithRatherLowProbability() && false) {
			// TODO: having clause does not come last
			select.setOrderByTerms(gen.getOrderingTerms());
		}
		select.setGroupByClause(gen.generateExpressions(Randomly.smallNumber() + 1));
		select.setHavingClause(null);
		String originalQueryString = CockroachDBVisitor.asString(select);
		
		List<String> resultSet = getResultSet(originalQueryString);
		
		CockroachDBExpression predicate = gen.generateHavingClause();
		String predicateString = CockroachDBVisitor.asString(predicate);
		String firstQueryString = String.format("%s HAVING (%s)", originalQueryString, predicateString);
		String secondQueryString = String.format("%s HAVING (NOT %s)", originalQueryString, predicateString);
		String thirdQueryString = String.format("%s HAVING (%s IS NULL)", originalQueryString, predicateString);
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		List<String> secondResultSet = getResultSet(combinedString);
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			state.getLogger().writeCurrent(combinedString);
		}
		if (resultSet.size() != secondResultSet.size()) {
			throw new AssertionError(originalQueryString + ";\n" + combinedString + ";");
		}
	}

	private List<String> getResultSet(String queryString) throws SQLException, AssertionError {
		QueryAdapter q = new QueryAdapter(queryString, errors);
		List<String> resultSet = new ArrayList<>();
		try (ResultSet result = q.executeAndGet(state.getConnection())) {
			if (result == null) {
				throw new IgnoreMeException();
			}
			while (result.next()) {
				resultSet.add(result.getString(1));
			}
		} catch (PSQLException e) {
			throw new AssertionError(queryString, e);
		}
		return resultSet;
	}

}
