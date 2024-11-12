package sqlancer.influxdb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.influxdb.InfluxDBErrors;
import sqlancer.influxdb.InfluxDBProvider.InfluxDBGlobalState;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBColumn;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBTable;
import sqlancer.influxdb.InfluxDBToStringVisitor;

public class InfluxDBInsertGenerator extends AbstractInsertGenerator<InfluxDBColumn> {

    private final InfluxDBGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public InfluxDBInsertGenerator(InfluxDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT "); // InfluxDB starts insert with "INSERT"
        InfluxDBTable table = globalState.getSchema().getRandomTable();
        List<InfluxDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(table.getName());
        sb.append(", ");
        insertColumns(columns);
        InfluxDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    public static SQLQueryAdapter getQuery(InfluxDBGlobalState globalState) {
        return new InfluxDBInsertGenerator(globalState).generate();
    }

    @Override
    protected void insertColumns(List<InfluxDBColumn> columns) {
        for (int nrColumn = 0; nrColumn < columns.size(); nrColumn++) {
            if (nrColumn != 0) {
                sb.append(", ");
            }
            insertValue(columns.get(nrColumn));
        }
    }

    @Override
    protected void insertValue(InfluxDBColumn influxDBColumn) {
        sb.append(InfluxDBToStringVisitor.asString(new InfluxDBExpressionGenerator(globalState).generateConstant()));
    }
}