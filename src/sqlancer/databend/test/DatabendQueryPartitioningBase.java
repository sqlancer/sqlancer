package sqlancer.databend.test;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.gen.DatabendExpressionGenerator;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendSchema.DatabendTables;
import sqlancer.databend.gen.DatabendNoRECExpressionGenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DatabendQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<DatabendExpression>, DatabendGlobalState> implements TestOracle {

    DatabendSchema s;
    DatabendTables targetTables;
    DatabendNoRECExpressionGenerator gen;
    DatabendSelect select;

    public DatabendQueryPartitioningBase(DatabendGlobalState state) {
        super(state);
        DatabendErrors.addExpressionErrors(errors);
    }

    public static String canonicalizeResultValue(String value) {
        // Rule: -0.0 should be canonicalized to 0.0
        if (Objects.equals(value, "-0.0")) {
            return "0.0";
        }

        return value;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new DatabendNoRECExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new DatabendSelect();
        select.setFetchColumns(generateFetchColumns());
        List<DatabendTable> tables = targetTables.getTables();
        List<TableReferenceNode<DatabendExpression, DatabendTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DatabendExpression, DatabendTable>(t)).collect(Collectors.toList());
        List<Node<DatabendExpression>> joins = DatabendJoin.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<Node<DatabendExpression>> generateFetchColumns() {
        List<Node<DatabendExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new DatabendColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<DatabendExpression, DatabendColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<DatabendExpression>> getGen() {
        return gen;
    }

}
