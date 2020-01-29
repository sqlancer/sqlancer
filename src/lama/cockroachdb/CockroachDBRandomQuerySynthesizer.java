package lama.cockroachdb;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTables;
import lama.cockroachdb.ast.CockroachDBSelect;

public class CockroachDBRandomQuerySynthesizer {
	
	public static Query generate(CockroachDBGlobalState globalState, int nrColumns) {
		CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
		CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState).setColumns(tables.getColumns());
		CockroachDBSelect select = new CockroachDBSelect();
		select.setDistinct(Randomly.getBoolean());
		select.setColumns(gen.generateExpressions(nrColumns));
		select.setFromTables(tables.getTables());
		if (Randomly.getBoolean()) {
			select.setWhereCondition(gen.generateExpression(CockroachDBDataType.BOOL.get()));
		}
		if (Randomly.getBoolean()) {
			if (select.isDistinct()) {
				// for SELECT DISTINCT, ORDER BY expressions must appear in select list
				// TODO: this error seems to only occur for column names, not constants
				select.setOrderByTerms(Randomly.nonEmptySubset(select.getColumns()));
			} else {
				select.setOrderByTerms(gen.getOrderingTerms());
			}
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
