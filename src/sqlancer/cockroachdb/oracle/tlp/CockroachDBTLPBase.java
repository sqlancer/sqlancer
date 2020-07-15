package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.TernaryLogicPartitioningOracleBase;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.cockroachdb.oracle.CockroachDBNoRECOracle;

public class CockroachDBTLPBase extends TernaryLogicPartitioningOracleBase<CockroachDBExpression>
        implements TestOracle {

    final CockroachDBGlobalState state;
    final Set<String> errors = new HashSet<>();

    CockroachDBSchema s;
    CockroachDBTables targetTables;
    CockroachDBExpressionGenerator gen;
    CockroachDBSelect select;

    public CockroachDBTLPBase(CockroachDBGlobalState state) {
        this.state = state;
        CockroachDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        select = new CockroachDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<CockroachDBTable> tables = targetTables.getTables();
        List<CockroachDBExpression> tableList = tables.stream().map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());
        List<CockroachDBExpression> joins = CockroachDBNoRECOracle.getJoins(tableList, state);
        select.setJoinList(joins);
        select.setFromList(tableList);
        select.setWhereClause(null);
        predicate = generatePredicate();
        negatedPredicate = new CockroachDBNotOperation(predicate);
        isNullPredicate = new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL);
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

    CockroachDBExpression generatePredicate() {
        return gen.generateExpression(CockroachDBDataType.BOOL.get());
    }

}
