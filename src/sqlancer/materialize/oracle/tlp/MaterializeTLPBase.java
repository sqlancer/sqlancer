package sqlancer.materialize.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeSchema.MaterializeTables;
import sqlancer.materialize.ast.MaterializeColumnValue;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeJoin;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.ast.MaterializeSelect.ForClause;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeFromTable;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeSubquery;
import sqlancer.materialize.gen.MaterializeCommon;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;
import sqlancer.materialize.oracle.MaterializeNoRECOracle;

public class MaterializeTLPBase
        extends TernaryLogicPartitioningOracleBase<MaterializeExpression, MaterializeGlobalState>
        implements TestOracle<MaterializeGlobalState> {

    protected MaterializeSchema s;
    protected MaterializeTables targetTables;
    protected MaterializeExpressionGenerator gen;
    protected MaterializeSelect select;

    public MaterializeTLPBase(MaterializeGlobalState state) {
        super(state);
        MaterializeCommon.addCommonExpressionErrors(errors);
        MaterializeCommon.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        List<MaterializeTable> tables = targetTables.getTables();
        List<MaterializeJoin> joins = getJoinStatements(state, targetTables.getColumns(), tables);
        generateSelectBase(tables, joins);
    }

    protected List<MaterializeJoin> getJoinStatements(MaterializeGlobalState globalState,
            List<MaterializeColumn> columns, List<MaterializeTable> tables) {
        return MaterializeNoRECOracle.getJoinStatements(state, columns, tables);
        // TODO joins
    }

    protected void generateSelectBase(List<MaterializeTable> tables, List<MaterializeJoin> joins) {
        List<MaterializeExpression> tableList = tables.stream()
                .map(t -> new MaterializeFromTable(t, Randomly.getBoolean())).collect(Collectors.toList());
        gen = new MaterializeExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new MaterializeSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses(joins);
        if (Randomly.getBoolean()) {
            select.setForClause(ForClause.getRandom());
        }
    }

    List<MaterializeExpression> generateFetchColumns() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return Arrays.asList(new MaterializeColumnValue(MaterializeColumn.createDummy("*"), null));
        }
        List<MaterializeExpression> fetchColumns = new ArrayList<>();
        List<MaterializeColumn> targetColumns = Randomly.nonEmptySubset(targetTables.getColumns());
        for (MaterializeColumn c : targetColumns) {
            fetchColumns.add(new MaterializeColumnValue(c, null));
        }
        return fetchColumns;
    }

    @Override
    protected ExpressionGenerator<MaterializeExpression> getGen() {
        return gen;
    }

    public static MaterializeSubquery createSubquery(MaterializeGlobalState globalState, String name,
            MaterializeTables tables) {
        List<MaterializeExpression> columns = new ArrayList<>();
        MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState)
                .setColumns(tables.getColumns());
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            columns.add(gen.generateExpression(0));
        }
        MaterializeSelect select = new MaterializeSelect();
        select.setFromList(tables.getTables().stream().map(t -> new MaterializeFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, MaterializeDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(MaterializeConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(
                        MaterializeConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(ForClause.getRandom());
        }
        return new MaterializeSubquery(select, name);
    }
}
