package sqlancer.influxdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.influxdb.InfluxDBSchema;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBColumn;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBTable;
import sqlancer.influxdb.ast.InfluxDBColumnReference;
import sqlancer.influxdb.ast.InfluxDBExpression;
import sqlancer.influxdb.ast.InfluxDBSelect;
import sqlancer.influxdb.ast.InfluxDBTableReference;
import sqlancer.influxdb.gen.InfluxDBExpressionGenerator;

public class InfluxDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<InfluxDBExpression, InfluxDBGlobalState>
        implements TestOracle<InfluxDBGlobalState> {

    InfluxDBSchema s;
    InfluxDBTable targetTable;
    InfluxDBExpressionGenerator gen;
    InfluxDBSelect select;

    protected InfluxDBQueryPartitioningBase(InfluxDBGlobalState state) {
        super(state);
        InfluxDBErrors.addExpressionErrors(errors);
    }

    List<InfluxDBExpression> generateFetchColumns() {
        List<InfluxDBExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new InfluxDBColumnReference(new InfluxDBColumn("*", null, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTable.getColumns()).stream().map(c -> new InfluxDBColumnReference(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<InfluxDBExpression> getGen() {
        return gen;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTable = s.getRandomTable();
        gen = new InfluxDBExpressionGenerator(state).setColumns(targetTable.getColumns());

        initializeTernaryPredicateVariants();

        select = new InfluxDBSelect();
        select.setFetchColumns(generateFetchColumns());

        List<InfluxDBTable> tables = new ArrayList<>();
        tables.add(targetTable);
        List<InfluxDBTableReference> tableList = tables.stream().map(t -> new InfluxDBTableReference(t))
                .collect(Collectors.toList());

        select.setFromList(new ArrayList<>(tableList));
        select.setWhereClause(null);
    }
}