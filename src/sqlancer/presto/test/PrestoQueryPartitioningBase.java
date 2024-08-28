package sqlancer.presto.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoSchema.PrestoTables;
import sqlancer.presto.ast.PrestoColumnReference;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.ast.PrestoTableReference;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public class PrestoQueryPartitioningBase extends TernaryLogicPartitioningOracleBase<PrestoExpression, PrestoGlobalState>
        implements TestOracle<PrestoGlobalState> {

    PrestoSchema s;
    PrestoTables targetTables;
    PrestoTypedExpressionGenerator gen;
    PrestoSelect select;

    public PrestoQueryPartitioningBase(PrestoGlobalState state) {
        super(state);
        PrestoErrors.addExpressionErrors(errors);
    }

    public static String canonicalizeResultValue(String value) {
        if (value == null) {
            return null;
        }

        // TODO: check this
        switch (value) {
        case "-0.0":
            return "0.0";
        case "-0":
            return "0";
        default:
        }

        return value;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new PrestoTypedExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new PrestoSelect();
        select.setFetchColumns(generateFetchColumns());
        List<PrestoTable> tables = targetTables.getTables();
        List<PrestoTableReference> tableList = tables.stream().map(t -> new PrestoTableReference(t))
                .collect(Collectors.toList());
        List<PrestoExpression> joins = PrestoJoin.getJoins(tableList, state);
        select.setJoinList(new ArrayList<>(joins));
        select.setFromList(new ArrayList<>(tableList));
        select.setWhereClause(null);
    }

    List<PrestoExpression> generateFetchColumns() {
        List<PrestoExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new PrestoColumnReference(new PrestoColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream().map(c -> new PrestoColumnReference(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<PrestoExpression> getGen() {
        return gen;
    }

}
