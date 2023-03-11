package sqlancer.doris.test;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.gen.DorisExpressionGenerator;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisSchema.DorisTables;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DorisQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<DorisExpression>, DorisGlobalState>
        implements TestOracle<DorisGlobalState> {

    DorisSchema s;
    DorisTables targetTables;
    DorisExpressionGenerator gen;
    DorisSelect select;

    public DorisQueryPartitioningBase(DorisGlobalState state) {
        super(state);
        DorisErrors.addExpressionErrors(errors);
    }

    public static String canonicalizeResultValue(String value) {
        if (value == null) {
            return value;
        }

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
        gen = new DorisExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new DorisSelect();
        select.setFetchColumns(generateFetchColumns());
        List<DorisTable> tables = targetTables.getTables();
        List<TableReferenceNode<DorisExpression, DorisTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DorisExpression, DorisTable>(t)).collect(Collectors.toList());
        List<Node<DorisExpression>> joins = DorisJoin.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<Node<DorisExpression>> generateFetchColumns() {
        List<Node<DorisExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new DorisColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<DorisExpression, DorisColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<DorisExpression>> getGen() {
        return gen;
    }

}
