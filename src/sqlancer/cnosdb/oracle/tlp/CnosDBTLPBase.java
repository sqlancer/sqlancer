package sqlancer.cnosdb.oracle.tlp;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTables;
import sqlancer.cnosdb.ast.CnosDBColumnValue;
import sqlancer.cnosdb.ast.CnosDBConstant;
import sqlancer.cnosdb.ast.CnosDBExpression;
import sqlancer.cnosdb.ast.CnosDBJoin;
import sqlancer.cnosdb.ast.CnosDBSelect;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBFromTable;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBSubquery;
import sqlancer.cnosdb.gen.CnosDBExpressionGenerator;
import sqlancer.cnosdb.oracle.CnosDBNoRECOracle;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;

public class CnosDBTLPBase extends TernaryLogicPartitioningOracleBase<CnosDBExpression, CnosDBGlobalState>
        implements TestOracle<CnosDBGlobalState> {

    protected CnosDBSchema s;
    protected CnosDBTables targetTables;
    protected CnosDBExpressionGenerator gen;
    protected CnosDBSelect select;

    public CnosDBTLPBase(CnosDBGlobalState state) {
        super(state);
    }

    public static CnosDBSubquery createSubquery(CnosDBGlobalState globalState, String name, CnosDBTables tables) {
        List<CnosDBExpression> columns = new ArrayList<>();
        CnosDBExpressionGenerator gen = new CnosDBExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            columns.add(gen.generateExpression(0));
        }
        CnosDBSelect select = new CnosDBSelect();
        select.setFromList(tables.getTables().stream().map(CnosDBFromTable::new).collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, CnosDBDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(CnosDBConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(CnosDBConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        return new CnosDBSubquery(select, name);
    }

    @Override
    public void check() throws Exception {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        List<CnosDBTable> tables = targetTables.getTables();
        List<CnosDBJoin> joins = getJoinStatements(targetTables.getColumns(), tables);
        generateSelectBase(tables, joins);
    }

    protected List<CnosDBJoin> getJoinStatements(List<CnosDBColumn> columns, List<CnosDBTable> tables) {
        return CnosDBNoRECOracle.getJoinStatements(state, columns, tables);
    }

    protected void generateSelectBase(List<CnosDBTable> tables, List<CnosDBJoin> joins) {
        List<CnosDBExpression> tableList = tables.stream().map(CnosDBFromTable::new).collect(Collectors.toList());
        gen = new CnosDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new CnosDBSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses(joins);
    }

    List<CnosDBExpression> generateFetchColumns() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return List.of(new CnosDBColumnValue(CnosDBColumn.createDummy("*")));
        }
        List<CnosDBExpression> fetchColumns = new ArrayList<>();
        List<CnosDBColumn> targetColumns = targetTables.getRandomColumnsWithOnlyOneField();

        ArrayList<CnosDBColumn> columns = new ArrayList<>();
        targetColumns.forEach(column -> column.getTable().getColumns().stream()
                .filter(field -> field instanceof CnosDBSchema.CnosDBFieldColumn).findFirst().ifPresent(columns::add));
        targetColumns.addAll(columns);

        targetColumns = targetColumns.stream().distinct().collect(Collectors.toList());

        for (CnosDBColumn c : targetColumns) {
            fetchColumns.add(new CnosDBColumnValue(c));
        }
        return fetchColumns;
    }

    @Override
    protected ExpressionGenerator<CnosDBExpression> getGen() {
        return gen;
    }

}
