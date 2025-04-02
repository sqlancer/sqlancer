package sqlancer.hive.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hive.HiveErrors;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveSchema;
import sqlancer.hive.HiveSchema.HiveColumn;
import sqlancer.hive.HiveSchema.HiveTable;
import sqlancer.hive.HiveSchema.HiveTables;
import sqlancer.hive.ast.HiveColumnReference;
import sqlancer.hive.ast.HiveExpression;
import sqlancer.hive.ast.HiveSelect;
import sqlancer.hive.ast.HiveTableReference;
import sqlancer.hive.gen.HiveExpressionGenerator;

public class HiveTLPBase extends TernaryLogicPartitioningOracleBase<HiveExpression, HiveGlobalState>
    implements TestOracle<HiveGlobalState> {

    HiveSchema schema;
    HiveTables targetTables;
    HiveExpressionGenerator gen;
    HiveSelect select;

    public HiveTLPBase(HiveGlobalState state) {
        super(state);
        HiveErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        schema = state.getSchema();
        targetTables = schema.getRandomTableNonEmptyTables();
        gen = new HiveExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new HiveSelect();
        select.setFetchColumns(generateFetchColumns());
        List<HiveTable> tables = targetTables.getTables();
        List<HiveTableReference> tableList = tables.stream().map(t -> new HiveTableReference(t))
                .collect(Collectors.toList());
        // List<HiveJoin> joins = HiveJoin.getJoins(tableList, state);
        // select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<HiveExpression> generateFetchColumns() {
        List<HiveExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new HiveColumnReference(new HiveColumn("*", null, null)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream().map(c -> new HiveColumnReference(c))
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<HiveExpression> getGen() {
        return gen;
    }
}
