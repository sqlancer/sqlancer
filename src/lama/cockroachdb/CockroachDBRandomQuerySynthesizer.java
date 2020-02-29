package lama.cockroachdb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTables;
import lama.cockroachdb.ast.CockroachDBExpression;
import lama.cockroachdb.ast.CockroachDBSelect;
import lama.cockroachdb.ast.CockroachDBTableReference;

public class CockroachDBRandomQuerySynthesizer {
	
	public static Query generate(CockroachDBGlobalState globalState, int nrColumns) {
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
		if (Randomly.getBoolean()) {
			select.setJoinList(CockroachDBMetamorphicQuerySynthesizer.getJoins(tableList, globalState));
		}
		select.setFromTables(tableList.stream().map(t -> (CockroachDBExpression) t).collect(Collectors.toList()));
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
		return new QueryAdapter(CockroachDBVisitor.asString(select));

	}

}
