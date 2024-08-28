package sqlancer.questdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;
import sqlancer.questdb.ast.QuestDBColumnReference;
import sqlancer.questdb.ast.QuestDBExpression;
import sqlancer.questdb.ast.QuestDBSelect;
import sqlancer.questdb.ast.QuestDBTableReference;
import sqlancer.questdb.gen.QuestDBExpressionGenerator;

public class QuestDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<QuestDBExpression, QuestDBGlobalState>
        implements TestOracle<QuestDBGlobalState> {

    QuestDBSchema s;
    QuestDBTable targetTable;
    QuestDBExpressionGenerator gen;
    QuestDBSelect select;

    protected QuestDBQueryPartitioningBase(QuestDBGlobalState state) {
        super(state);
        QuestDBErrors.addExpressionErrors(errors);
    }

    List<QuestDBExpression> generateFetchColumns() {
        List<QuestDBExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new QuestDBColumnReference(new QuestDBColumn("*", null, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTable.getColumns()).stream().map(c -> new QuestDBColumnReference(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<QuestDBExpression> getGen() {
        return gen;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        // Only return one table instead of multiple tables, which is regarded as illegal by QuestDB
        // e.g. "SELECT * FROM t0, t1;"
        targetTable = s.getRandomTable();
        gen = new QuestDBExpressionGenerator(state).setColumns(targetTable.getColumns());
        initializeTernaryPredicateVariants();
        select = new QuestDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<QuestDBTable> tables = new ArrayList<>();
        tables.add(targetTable);
        List<QuestDBTableReference> tableList = tables.stream().map(t -> new QuestDBTableReference(t))
                .collect(Collectors.toList());
        // Ignore JOINs for now
        select.setFromList(new ArrayList<>(tableList));
        select.setWhereClause(null);
    }
}
