package sqlancer.questdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;
import sqlancer.questdb.QuestDBSchema.QuestDBTables;
import sqlancer.questdb.ast.QuestDBExpression;
import sqlancer.questdb.ast.QuestDBSelect;
import sqlancer.questdb.gen.QuestDBExpressionGenerator;

public class QuestDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<QuestDBExpression>, QuestDBGlobalState>
        implements TestOracle<QuestDBGlobalState> {

    QuestDBSchema s;
    QuestDBTables targetTables;
    QuestDBExpressionGenerator gen;
    QuestDBSelect select;

    protected QuestDBQueryPartitioningBase(QuestDBGlobalState state) {
        super(state);
        QuestDBErrors.addExpressionErrors(errors);
    }

    List<Node<QuestDBExpression>> generateFetchColumns() {
        List<Node<QuestDBExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new QuestDBColumn("*", null, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<QuestDBExpression, QuestDBColumn>(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    public static String canonicalizeResultValue(String value) {
        // Rule: -0.0 should be canonicalized to 0.0
        if (Objects.equals(value, "-0.0")) {
            return "0.0";
        }

        return value;
    }

    @Override
    protected ExpressionGenerator<Node<QuestDBExpression>> getGen() {
        return gen;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new QuestDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new QuestDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<QuestDBTable> tables = targetTables.getTables();
        List<TableReferenceNode<QuestDBExpression, QuestDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<QuestDBExpression, QuestDBTable>(t)).collect(Collectors.toList());
        // Ignore JOINs for now
        select.setFromList(new ArrayList<>(tableList));
        select.setWhereClause(null);
    }
}
