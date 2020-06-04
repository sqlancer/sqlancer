package sqlancer.clickhouse.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.TestOracle;
import sqlancer.ast.ClickhouseSelect;
import sqlancer.ast.newast.ColumnReferenceNode;
import sqlancer.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.ast.newast.Node;
import sqlancer.ast.newast.TableReferenceNode;
import sqlancer.clickhouse.ClickhouseErrors;
import sqlancer.clickhouse.ClickhouseProvider.ClickhouseGlobalState;
import sqlancer.clickhouse.ClickhouseSchema;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseTable;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseTables;
import sqlancer.clickhouse.ast.ClickhouseExpression;
import sqlancer.clickhouse.gen.ClickhouseExpressionGenerator;
import sqlancer.clickhouse.gen.ClickhouseExpressionGenerator.ClickhouseUnaryPostfixOperator;
import sqlancer.clickhouse.gen.ClickhouseExpressionGenerator.ClickhouseUnaryPrefixOperator;

public class ClickhouseQueryPartitioningBase implements TestOracle {

    final ClickhouseGlobalState state;
    final Set<String> errors = new HashSet<>();

    ClickhouseSchema s;
    ClickhouseTables targetTables;
    ClickhouseExpressionGenerator gen;
    ClickhouseSelect select;
    Node<ClickhouseExpression> predicate;
    Node<ClickhouseExpression> negatedPredicate;
    Node<ClickhouseExpression> isNullPredicate;

    public ClickhouseQueryPartitioningBase(ClickhouseGlobalState state) {
        this.state = state;
        ClickhouseErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new ClickhouseExpressionGenerator(state).setColumns(targetTables.getColumns());
        select = new ClickhouseSelect();
        select.setFetchColumns(generateFetchColumns());
        List<ClickhouseTable> tables = targetTables.getTables();
        List<Node<ClickhouseExpression>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<ClickhouseExpression, ClickhouseTable>(t))
                .collect(Collectors.toList());
        // TODO join
        select.setFromList(tableList);
        select.setWhereClause(null);
        predicate = generatePredicate();
        negatedPredicate = new NewUnaryPrefixOperatorNode<>(predicate, ClickhouseUnaryPrefixOperator.NOT);
        isNullPredicate = new NewUnaryPostfixOperatorNode<>(predicate, ClickhouseUnaryPostfixOperator.IS_NULL);
    }

    List<Node<ClickhouseExpression>> generateFetchColumns() {
        return Arrays.asList(new ColumnReferenceNode<>(targetTables.getColumns().get(0)));
    }

    Node<ClickhouseExpression> generatePredicate() {
        return gen.generateExpression();
    }

}
