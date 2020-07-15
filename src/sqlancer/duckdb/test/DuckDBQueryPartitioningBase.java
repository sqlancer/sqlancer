package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.TernaryLogicPartitioningOracleBase;
import sqlancer.TestOracle;
import sqlancer.ast.newast.ColumnReferenceNode;
import sqlancer.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.ast.newast.Node;
import sqlancer.ast.newast.TableReferenceNode;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBUnaryPostfixOperator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBUnaryPrefixOperator;

public class DuckDBQueryPartitioningBase extends TernaryLogicPartitioningOracleBase<Node<DuckDBExpression>>
        implements TestOracle {

    final DuckDBGlobalState state;
    final Set<String> errors = new HashSet<>();

    DuckDBSchema s;
    DuckDBTables targetTables;
    DuckDBExpressionGenerator gen;
    DuckDBSelect select;

    public DuckDBQueryPartitioningBase(DuckDBGlobalState state) {
        this.state = state;
        DuckDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new DuckDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        select = new DuckDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<DuckDBTable> tables = targetTables.getTables();
        List<TableReferenceNode<DuckDBExpression, DuckDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DuckDBExpression, DuckDBTable>(t)).collect(Collectors.toList());
        List<Node<DuckDBExpression>> joins = DuckDBJoin.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
        predicate = generatePredicate();
        negatedPredicate = new NewUnaryPrefixOperatorNode<>(predicate, DuckDBUnaryPrefixOperator.NOT);
        isNullPredicate = new NewUnaryPostfixOperatorNode<>(predicate, DuckDBUnaryPostfixOperator.IS_NULL);
    }

    List<Node<DuckDBExpression>> generateFetchColumns() {
        List<Node<DuckDBExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new DuckDBColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    Node<DuckDBExpression> generatePredicate() {
        return gen.generateExpression();
    }

}
