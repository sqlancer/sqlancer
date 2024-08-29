package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.ast.DuckDBColumnReference;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTableReference;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;

public class DuckDBQueryPartitioningBase extends TernaryLogicPartitioningOracleBase<DuckDBExpression, DuckDBGlobalState>
        implements TestOracle<DuckDBGlobalState> {

    DuckDBSchema s;
    DuckDBTables targetTables;
    DuckDBExpressionGenerator gen;
    DuckDBSelect select;

    public DuckDBQueryPartitioningBase(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new DuckDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new DuckDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<DuckDBTable> tables = targetTables.getTables();
        List<DuckDBTableReference> tableList = tables.stream().map(t -> new DuckDBTableReference(t))
                .collect(Collectors.toList());
        List<DuckDBJoin> joins = DuckDBJoin.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<DuckDBExpression> generateFetchColumns() {
        List<DuckDBExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new DuckDBColumnReference(new DuckDBColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream().map(c -> new DuckDBColumnReference(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<DuckDBExpression> getGen() {
        return gen;
    }

}
