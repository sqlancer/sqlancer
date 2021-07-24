package sqlancer.mongodb.test;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.DocumentRemovalOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mongodb.MongoDBProvider;
import sqlancer.mongodb.MongoDBSchema;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.gen.MongoDBComputedExpressionGenerator;
import sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator;

public class MongoDBDocumentRemovalBase extends
        DocumentRemovalOracleBase<Node<MongoDBExpression>, MongoDBProvider.MongoDBGlobalState> implements TestOracle {

    protected MongoDBSchema schema;
    protected MongoDBSchema.MongoDBTables targetTables;
    protected MongoDBSchema.MongoDBTable mainTable;
    protected List<MongoDBColumnTestReference> targetColumns;
    protected MongoDBMatchExpressionGenerator expressionGenerator;
    protected MongoDBSelect<MongoDBExpression> select;

    protected MongoDBDocumentRemovalBase(MongoDBProvider.MongoDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        schema = state.getSchema();
        targetTables = schema.getRandomTableNonEmptyTables();
        mainTable = targetTables.getTables().get(0);
        generateTargetColumns();
        expressionGenerator = new MongoDBMatchExpressionGenerator(state).setColumns(targetColumns);
        initializeDocumentRemovalOracle();
        select = new MongoDBSelect<>(mainTable.getName(), targetColumns.get(0));
        select.setProjectionList(targetColumns);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setLookupList(targetColumns);
        } else {
            select.setLookupList(Randomly.nonEmptySubset(targetColumns));
        }
        if (state.getDbmsSpecificOptions().testComputedValues) {
            generateComputedColumns();
        }
    }

    private void generateTargetColumns() {
        targetColumns = new ArrayList<>();
        for (MongoDBSchema.MongoDBColumn c : mainTable.getColumns()) {
            targetColumns.add(new MongoDBColumnTestReference(c, true));
        }
        List<MongoDBColumnTestReference> joinsOtherTables = new ArrayList<>();
        if (!state.getDbmsSpecificOptions().nullSafety) {
            for (int i = 1; i < targetTables.getTables().size(); i++) {
                MongoDBSchema.MongoDBTable procTable = targetTables.getTables().get(i);
                for (MongoDBSchema.MongoDBColumn c : procTable.getColumns()) {
                    joinsOtherTables.add(new MongoDBColumnTestReference(c, false));
                }
            }
        }
        if (!joinsOtherTables.isEmpty()) {
            int randNumber = state.getRandomly().getInteger(1, Math.min(joinsOtherTables.size(), 4));
            List<MongoDBColumnTestReference> subsetJoinsOtherTables = Randomly.nonEmptySubset(joinsOtherTables,
                    randNumber);
            targetColumns.addAll(subsetJoinsOtherTables);
        }
    }

    private void generateComputedColumns() {
        List<Node<MongoDBExpression>> computedColumns = new ArrayList<>();
        int numberComputedColumns = state.getRandomly().getInteger(1, 4);
        MongoDBComputedExpressionGenerator generator = new MongoDBComputedExpressionGenerator(state)
                .setColumns(targetColumns);
        for (int i = 0; i < numberComputedColumns; i++) {
            computedColumns.add(generator.generateExpression());
        }
        select.setComputedClause(computedColumns);
    }

    @Override
    protected ExpressionGenerator<Node<MongoDBExpression>> getGen() {
        return expressionGenerator;
    }
}
