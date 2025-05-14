package sqlancer.oxla.schema;

import org.postgresql.util.PGInterval;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractTables;
import sqlancer.oxla.ast.OxlaConstant;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OxlaTables extends AbstractTables<OxlaTable, OxlaColumn> {
    public OxlaTables(List<OxlaTable> tables) {
        super(tables);
    }

    public OxlaRowValue getRandomRowValue(SQLConnection connection) {
        final String randomRowQuery = String.format("SELECT %s FROM %s ORDER BY random() LIMIT 1",
                columnNamesAsString(c -> String
                        .format("%s.%s AS %s_%s",
                                c.getTable().getName(),
                                c.getName(),
                                c.getTable().getName(),
                                c.getName())),
                tableNamesAsString()
        );
        final Map<OxlaColumn, OxlaConstant> values = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            final ResultSet randomRowValues = statement.executeQuery(randomRowQuery);
            if (!randomRowValues.next()) {
                throw new AssertionError("OxlaTables::getRandomRowValue failed to find a random row.");
            }
            for (int index = 0; index < getColumns().size(); ++index) {
                final OxlaColumn column = getColumns().get(index);
                int columnIndex = randomRowValues.findColumn(String.format("%s_%s", column.getTable().getName(), column.getName()));
                assert columnIndex == index + 1;
                OxlaConstant constant = null;
                if (randomRowValues.getString(columnIndex) == null) {
                    constant = OxlaConstant.createNullConstant();
                } else {
                    switch (column.getType()) {
                        case BOOLEAN: {
                            constant = OxlaConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                            break;
                        }
                        case DATE: {
                            constant = OxlaConstant.createDateConstant((int) randomRowValues.getDate(columnIndex).getTime());
                            break;
                        }
                        case FLOAT32: {
                            constant = OxlaConstant.createFloat32Constant(randomRowValues.getFloat(columnIndex));
                            break;
                        }
                        case FLOAT64: {
                            constant = OxlaConstant.createFloat64Constant(randomRowValues.getDouble(columnIndex));
                            break;
                        }
                        case INT32: {
                            constant = OxlaConstant.createInt32Constant(randomRowValues.getInt(columnIndex));
                            break;
                        }
                        case INT64: {
                            constant = OxlaConstant.createInt64Constant(randomRowValues.getLong(columnIndex));
                            break;
                        }
                        case INTERVAL: {
                            final PGInterval interval = randomRowValues.getObject(columnIndex, PGInterval.class);
                            constant = OxlaConstant.createIntervalConstant(interval.getMonths(), interval.getDays(), interval.getMicroSeconds());
                            break;
                        }
                        case JSON: {
                            constant = OxlaConstant.createJsonConstant(randomRowValues.getString(columnIndex));
                            break;
                        }
                        case TEXT: {
                            constant = OxlaConstant.createTextConstant(randomRowValues.getString(columnIndex));
                            break;
                        }
                        case TIME: {
                            constant = OxlaConstant.createTimeConstant((int) Time.valueOf(randomRowValues.getString(columnIndex)).getTime());
                            break;
                        }
                        case TIMESTAMP: {
                            try {
                                final Date parsedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                        .parse(randomRowValues.getString(columnIndex));
                                constant = OxlaConstant.createTimestampConstant(parsedTimestamp.getTime());
                            } catch (ParseException e) {
                                throw new AssertionError("OxlaTables::getRandomRowValue: failed to parse timestamp without time zone: %s", e);
                            }
                            break;
                        }
                        case TIMESTAMPTZ: {
                            try {
                                final Date parsedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss+00")
                                        .parse(randomRowValues.getString(columnIndex));
                                constant = OxlaConstant.createTimestamptzConstant(parsedTimestamp.getTime());
                            } catch (ParseException e) {
                                throw new AssertionError("OxlaTables::getRandomRowValue: failed to parse timestamp with time zone: %s", e);
                            }
                            break;
                        }
                        default:
                            throw new AssertionError(String.format("OxlaTables::getRandomRowValue unhandled type: %s", column.getType()));
                    }
                }
                values.put(column, constant);
            }
            if (randomRowValues.next()) {
                throw new AssertionError("OxlaTables::getRandomRowValue returned more than 1 row.");
            }
        } catch (SQLException e) {
            throw new AssertionError(String.format("OxlaTables::getRandomRowValue failed: %s", e));
        }
        return new OxlaRowValue(this, values);
    }
}
