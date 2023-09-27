package sqlancer.presto.test;

import java.sql.SQLException;
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
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoSchema.PrestoTables;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public class PrestoQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<PrestoExpression>, PrestoGlobalState>
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
        List<TableReferenceNode<PrestoExpression, PrestoTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<PrestoExpression, PrestoTable>(t)).collect(Collectors.toList());
        List<Node<PrestoExpression>> joins = PrestoJoin.getJoins(tableList, state);
        select.setJoinList(new ArrayList<>(joins));
        select.setFromList(new ArrayList<>(tableList));
        select.setWhereClause(null);
    }

    List<Node<PrestoExpression>> generateFetchColumns() {
        List<Node<PrestoExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new PrestoColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<PrestoExpression, PrestoColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<PrestoExpression>> getGen() {
        return gen;
    }

}
