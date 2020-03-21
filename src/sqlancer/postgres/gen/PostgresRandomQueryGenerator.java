package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.SelectType;

public class PostgresRandomQueryGenerator {

	public static PostgresSelect createRandomQuery(int nrColumns, PostgresSchema newSchema, Randomly r) {
		List<PostgresExpression> columns = new ArrayList<>();
		PostgresTables tables = newSchema.getRandomTableNonEmptyTables();
		PostgresExpressionGenerator gen = new PostgresExpressionGenerator(r).setColumns(tables.getColumns());
		for (int i = 0; i < nrColumns; i++) {
			columns.add(gen.generateExpression(0));
		}
		PostgresSelect select = new PostgresSelect();
		select.setSelectType(SelectType.getRandom());
		if (select.getSelectOption() == SelectType.DISTINCT && Randomly.getBoolean()) {
			select.setDistinctOnClause(gen.generateExpression(0));
		}
		select.setFromList(tables.getTables().stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean())).collect(Collectors.toList()));
		select.setFetchColumns(columns);
		if (Randomly.getBoolean()) {
			select.setWhereClause(gen.generateExpression(0, PostgresDataType.BOOLEAN));
		}
		if (Randomly.getBooleanWithSmallProbability()) {
			select.setGroupByClause(gen.generateExpressions(Randomly.smallNumber() + 1));
		}
		if (Randomly.getBooleanWithSmallProbability()) {
			select.setOrderByClause(gen.generateOrderBy());
		}
		if (Randomly.getBoolean()) {
			select.setLimitClause(PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
			if (Randomly.getBoolean()) {
				select.setOffsetClause(PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
			}
		}
		return select;
	}

}
