package influxdb.gen;

import influxdb.InfluxDBGlobalState;
import sql.SQLQueryAdapter;
import java.util.Map;
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

public class InfluxDBWritePointGenerator {

    public static SQLQueryAdapter getQuery(InfluxDBGlobalState globalState, String measurement, 
                                           Map<String, String> tags, Map<String, Object> fields) throws Exception {
        
        // Construct the base part of the query with measurement name
        StringBuilder queryBuilder = new StringBuilder(measurement);

        // Add tags to the query
        if (tags != null && !tags.isEmpty()) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                queryBuilder.append(",").append(entry.getKey()).append("=").append(entry.getValue());
            }
        }

        // Add fields to the query, ensuring at least one field is present
        if (fields != null && !fields.isEmpty()) {
            queryBuilder.append(" ");
            boolean firstField = true;
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                if (!firstField) {
                    queryBuilder.append(",");
                }
                queryBuilder.append(entry.getKey()).append("=").append(entry.getValue());
                firstField = false;
            }
        } else {
            throw new Exception("Fields cannot be empty for an InfluxDB write operation.");
        }

        // Add timestamp to the query (optional)
        long timestamp = System.currentTimeMillis();
        queryBuilder.append(" ").append(timestamp);

        // Create and return SQLQueryAdapter with the generated query string
        String query = queryBuilder.toString();
        return new SQLQueryAdapter(query, false);
    }
}