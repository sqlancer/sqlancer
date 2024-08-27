package sqlancer.hsqldb.test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBColumnReference;
import sqlancer.hsqldb.ast.HSQLDBExpression;
//import sqlancer.hsqldb.ast.HSQLDBJoin;
import sqlancer.hsqldb.ast.HSQLDBSelect;
import sqlancer.hsqldb.ast.HSQLDBTableReference;
import sqlancer.hsqldb.gen.HSQLDBExpressionGenerator;

public class HSQLDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<HSQLDBExpression, HSQLDBProvider.HSQLDBGlobalState>
        implements TestOracle<HSQLDBProvider.HSQLDBGlobalState> {

    HSQLDBSelect select;
    HSQLDBExpressionGenerator expressionGenerator;

    HSQLDBSchema schema;

    List<HSQLDBSchema.HSQLDBTable> targetTables;

    public HSQLDBQueryPartitioningBase(HSQLDBProvider.HSQLDBGlobalState state) {
        super(state);
    }

    @Override
    protected ExpressionGenerator<HSQLDBExpression> getGen() {
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
        List<HSQLDBTableReference> tableList = targetTables.stream().map(t -> new HSQLDBTableReference(t))
                .collect(Collectors.toList());
        // List<Node<HSQLDBExpression>> joins = HSQLDBJoin.getJoins(tableList, state);
        // select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);

        ComparatorHelper.getResultSetFirstColumnAsString(HSQLDBToStringVisitor.asString(select), errors, state);

    }

    List<HSQLDBExpression> generateFetchColumns() {
        List<HSQLDBExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new HSQLDBColumnReference(new HSQLDBSchema.HSQLDBColumn("*", null, null)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.stream().flatMap(t -> t.getColumns().stream())
                    .map(c -> new HSQLDBColumnReference(c)).collect(Collectors.toList()));
        }
        return columns;
    }
}
