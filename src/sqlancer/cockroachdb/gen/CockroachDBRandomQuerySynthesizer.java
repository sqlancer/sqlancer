package sqlancer.cockroachdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.test.CockroachDBNoRECTester;

public class CockroachDBRandomQuerySynthesizer {
	
	public static Query generate(CockroachDBGlobalState globalState, int nrColumns) {
		CockroachDBSelect select = generateSelect(globalState, nrColumns);
		return new QueryAdapter(CockroachDBVisitor.asString(select));

	}

	public static CockroachDBSelect generateSelect(CockroachDBGlobalState globalState, int nrColumns)
			throws AssertionError {
		CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
		CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState).setColumns(tables.getColumns());
		CockroachDBSelect select = new CockroachDBSelect();
		select.setDistinct(Randomly.getBoolean());
		boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
		List<CockroachDBExpression> columns = new ArrayList<>();
		List<CockroachDBExpression> columnsWithoutAggregates = new ArrayList<>();
		for (int i = 0; i < nrColumns; i++) {
			if (allowAggregates && Randomly.getBoolean()) {
				CockroachDBExpression expression = gen.generateExpression(CockroachDBDataType.getRandom().get());
				columns.add(expression);
				columnsWithoutAggregates.add(expression);
			} else {
				columns.add(gen.generateAggregate());
			}
		}
		select.setColumns(columns);
		List<CockroachDBTableReference> tableList = tables.getTables().stream().map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
		List<CockroachDBTableReference> updatedTableList = CockroachDBCommon.getTableReferences(tableList);
		if (Randomly.getBoolean()) {
			select.setJoinList(CockroachDBNoRECTester.getJoins(updatedTableList, globalState));
		}
		select.setFromTables(updatedTableList);
		if (Randomly.getBoolean()) {
			select.setWhereCondition(gen.generateExpression(CockroachDBDataType.BOOL.get()));
		}
		if (Randomly.getBoolean()) {
			if (select.isDistinct()) {
				// for SELECT DISTINCT, ORDER BY expressions must appear in select list
				// TODO: this error seems to only occur for column names, not constants
				if (!columnsWithoutAggregates.isEmpty()) {
					select.setOrderByTerms(Randomly.nonEmptySubset(columnsWithoutAggregates));
				}
			} else {
				select.setOrderByTerms(gen.getOrderingTerms());
			}
		}
		if (Randomly.getBoolean() || !select.getOrderByTerms().isEmpty()) {
			// <select column > must appear in the GROUP BY clause or be used in an aggregate function
			List<CockroachDBExpression> groupBys = new ArrayList<>(columnsWithoutAggregates);
			// must appear in the GROUP BY clause or be used in an aggregate function
			groupBys.addAll(select.getOrderByTerms());
			while (Randomly.getBooleanWithRatherLowProbability()) {
				groupBys.add(gen.generateExpression(CockroachDBDataType.getRandom().get()));
			}
			select.setGroupByClause(groupBys);
			
		}

		if (Randomly.getBoolean()) { // TODO expression
			select.setLimitTerm(gen.generateConstant(CockroachDBDataType.INT.get()));
		}
		if (Randomly.getBoolean()) { // TODO expression
			/* https://github.com/cockroachdb/cockroach/issues/44203 */
			select.setOffset(gen.generateConstant(CockroachDBDataType.INT.get()));
		}
		if (Randomly.getBoolean() && false) {
			select.setHavingClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
		}
		return select;
	}

}
