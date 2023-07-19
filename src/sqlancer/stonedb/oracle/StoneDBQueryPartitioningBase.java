package sqlancer.stonedb.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.stonedb.StoneDBErrors;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBSchema.StoneDBTables;
import sqlancer.stonedb.ast.StoneDBExpression;
import sqlancer.stonedb.ast.StoneDBJoin;
import sqlancer.stonedb.ast.StoneDBSelect;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator;

public class StoneDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<StoneDBExpression>, StoneDBGlobalState>
        implements TestOracle<StoneDBGlobalState> {

    StoneDBSchema schema;
    StoneDBTables targetTables;
    StoneDBExpressionGenerator gen;
    StoneDBSelect select;

    public StoneDBQueryPartitioningBase(StoneDBGlobalState state) {
        super(state);
        StoneDBErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        schema = state.getSchema();
        targetTables = schema.getRandomTableNonEmptyTables();
        gen = new StoneDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();

        select = new StoneDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<StoneDBTable> tables = targetTables.getTables();
        List<TableReferenceNode<StoneDBExpression, StoneDBTable>> tableReferenceNodeList = tables.stream()
                .map(t -> new TableReferenceNode<StoneDBExpression, StoneDBTable>(t)).collect(Collectors.toList());
        List<Node<StoneDBExpression>> joins = StoneDBJoin.getJoins(tableReferenceNodeList, state);
        select.setJoinList(new ArrayList<>(joins));
        select.setFromList(new ArrayList<>(tableReferenceNodeList));
        select.setWhereClause(null);
    }

    List<Node<StoneDBExpression>> generateFetchColumns() {
        List<Node<StoneDBExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new StoneDBColumn("*", null, false, false, 0)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<StoneDBExpression, StoneDBColumn>(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<StoneDBExpression>> getGen() {
        return gen;
    }
}
