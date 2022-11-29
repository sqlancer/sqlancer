package sqlancer.hsqldb.test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBExpression;
//import sqlancer.hsqldb.ast.HSQLDBJoin;
import sqlancer.hsqldb.ast.HSQLDBSelect;
import sqlancer.hsqldb.gen.HSQLDBExpressionGenerator;

public class HSQLDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<HSQLDBExpression>, HSQLDBProvider.HSQLDBGlobalState>
        implements TestOracle {

    HSQLDBSelect select;
    HSQLDBExpressionGenerator expressionGenerator;

    HSQLDBSchema schema;

    List<HSQLDBSchema.HSQLDBTable> targetTables;

    public HSQLDBQueryPartitioningBase(HSQLDBProvider.HSQLDBGlobalState state) {
        super(state);
    }

    @Override
    protected ExpressionGenerator<Node<HSQLDBExpression>> getGen() {
        return expressionGenerator;
    }

    @Override
    public void check() throws Exception {
        schema = state.getSchema();
        targetTables = schema.getDatabaseTablesRandomSubsetNotEmpty();
        expressionGenerator = new HSQLDBExpressionGenerator(state)
                .setColumns(targetTables.stream().flatMap(t -> t.getColumns().stream()).collect(Collectors.toList()));
        initializeTernaryPredicateVariants();
        select = new HSQLDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<TableReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBTable>> tableList = targetTables.stream()
                .map(t -> new TableReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBTable>(t))
                .collect(Collectors.toList());
        // List<Node<HSQLDBExpression>> joins = HSQLDBJoin.getJoins(tableList, state);
        // select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);

        ComparatorHelper.getResultSetFirstColumnAsString(HSQLDBToStringVisitor.asString(select), errors, state);

    }

    List<Node<HSQLDBExpression>> generateFetchColumns() {
        List<Node<HSQLDBExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new HSQLDBSchema.HSQLDBColumn("*", null, null)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.stream().flatMap(t -> t.getColumns().stream())
                    .map(c -> new ColumnReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBColumn>(c))
                    .collect(Collectors.toList()));
        }
        return columns;
    }
}
