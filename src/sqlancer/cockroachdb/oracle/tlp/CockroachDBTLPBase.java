package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.cockroachdb.oracle.CockroachDBNoRECOracle;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;

public class CockroachDBTLPBase extends
        TernaryLogicPartitioningOracleBase<CockroachDBExpression, CockroachDBGlobalState> implements TestOracle {

    CockroachDBSchema s;
    CockroachDBTables targetTables;
    CockroachDBExpressionGenerator gen;
    CockroachDBSelect select;

    public CockroachDBTLPBase(CockroachDBGlobalState state) {
        super(state);
        CockroachDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new CockroachDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<CockroachDBTable> tables = targetTables.getTables();
        List<CockroachDBExpression> tableList = tables.stream().map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());
        List<CockroachDBExpression> joins = CockroachDBNoRECOracle.getJoins(tableList, state);
        select.setJoinList(joins);
        select.setFromList(tableList);
        select.setWhereClause(null);
    }

    List<CockroachDBExpression> generateFetchColumns() {
        List<CockroachDBExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new CockroachDBColumnReference(new CockroachDBColumn("*", null, false, false)));
        } else {
            columns.addAll(Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new CockroachDBColumnReference(c)).collect(Collectors.toList()));
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<CockroachDBExpression> getGen() {
        return gen;
    }

}
