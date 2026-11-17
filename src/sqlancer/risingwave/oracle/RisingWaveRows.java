package sqlancer.risingwave.oracle;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.postgresql.util.PGInterval;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.risingwave.RisingWaveGlobalState;

/** Exact unordered complete-row multisets. Metadata is retained even for empty results. */
public final class RisingWaveRows {
    private enum Type {
        NUMBER, BOOLEAN, TEXT, DATE, TIME, TIMESTAMP, INTERVAL, NUMBER_ARRAY, BOOLEAN_ARRAY, TEXT_ARRAY
    }

    private final List<Type> columns;
    private final Map<List<Object>, Long> rows;

    private RisingWaveRows(List<Type> columns, Map<List<Object>, Long> rows) {
        this.columns = List.copyOf(columns);
        this.rows = new HashMap<>(rows);
    }

    public boolean sameMetadata(RisingWaveRows other) {
        return columns.equals(other.columns);
    }

    public void requireSameMetadata(RisingWaveRows other) {
        if (!sameMetadata(other)) {
            throw new AssertionError("Incompatible RisingWave result metadata: " + columns + " / " + other.columns);
        }
    }

    public boolean same(RisingWaveRows other) {
        requireSameMetadata(other);
        return rows.equals(other.rows);
    }

    @Override
    public String toString() {
        return "columns=" + columns + " rows=" + rows;
    }

    public RisingWaveRows plus(RisingWaveRows other) {
        requireSameMetadata(other);
        Map<List<Object>, Long> combined = new HashMap<>(rows);
        other.rows.forEach((row, count) -> combined.merge(row, count, Math::addExact));
        return new RisingWaveRows(columns, combined);
    }

    public static RisingWaveRows read(ResultSet result) throws SQLException {
        ResultSetMetaData metadata = result.getMetaData();
        if (metadata == null) {
            throw new SQLException("Missing RisingWave result metadata");
        }
        int width = metadata.getColumnCount();
        if (width < 1) {
            throw new SQLException("Missing RisingWave result columns");
        }
        List<Type> columns = new ArrayList<>();
        for (int column = 1; column <= width; column++) {
            columns.add(type(metadata.getColumnType(column), metadata.getColumnTypeName(column)));
        }
        Map<List<Object>, Long> rows = new HashMap<>();
        while (result.next()) {
            List<Object> row = new ArrayList<>();
            for (int column = 1; column <= width; column++) {
                row.add(value(result, column, columns.get(column - 1)));
            }
            // List.copyOf rejects the null values used for SQL NULL.
            rows.merge(Collections.unmodifiableList(row), 1L, Math::addExact);
        }
        return new RisingWaveRows(columns, rows);
    }

    private static Type type(int jdbc, String name) throws SQLException {
        if (name == null) {
            throw new SQLException("Missing RisingWave result type name");
        }
        switch (name.toLowerCase(Locale.ROOT)) {
        case "int2":
        case "smallint":
            if (jdbc == Types.SMALLINT) {
                return Type.NUMBER;
            }
            break;
        case "int4":
        case "integer":
            if (jdbc == Types.INTEGER) {
                return Type.NUMBER;
            }
            break;
        case "int8":
        case "bigint":
            if (jdbc == Types.BIGINT) {
                return Type.NUMBER;
            }
            break;
        case "numeric":
        case "decimal":
            if (jdbc == Types.NUMERIC || jdbc == Types.DECIMAL) {
                return Type.NUMBER;
            }
            break;
        case "bool":
        case "boolean":
            if (jdbc == Types.BOOLEAN || jdbc == Types.BIT) {
                return Type.BOOLEAN;
            }
            break;
        case "varchar":
        case "text":
            if (jdbc == Types.VARCHAR) {
                return Type.TEXT;
            }
            break;
        case "date":
            if (jdbc == Types.DATE) {
                return Type.DATE;
            }
            break;
        case "time":
            if (jdbc == Types.TIME) {
                return Type.TIME;
            }
            break;
        case "timestamp":
            if (jdbc == Types.TIMESTAMP) {
                return Type.TIMESTAMP;
            }
            break;
        case "interval":
            if (jdbc == Types.OTHER) {
                return Type.INTERVAL;
            }
            break;
        case "_int4":
        case "_int8":
            if (jdbc == Types.ARRAY) {
                return Type.NUMBER_ARRAY;
            }
            break;
        case "_bool":
            if (jdbc == Types.ARRAY) {
                return Type.BOOLEAN_ARRAY;
            }
            break;
        case "_varchar":
        case "_text":
            if (jdbc == Types.ARRAY) {
                return Type.TEXT_ARRAY;
            }
            break;
        default:
            break;
        }
        throw new SQLException("Unsupported RisingWave result metadata: " + jdbc + " / " + name);
    }

    private static Object value(ResultSet result, int column, Type type) throws SQLException {
        switch (type) {
        case NUMBER:
            BigDecimal number = result.getBigDecimal(column);
            return number == null ? null : number.stripTrailingZeros();
        case TEXT:
            return result.getString(column);
        case BOOLEAN:
            boolean bool = result.getBoolean(column);
            return result.wasNull() ? null : bool;
        case DATE:
            return result.getObject(column, LocalDate.class);
        case TIME:
            return result.getObject(column, LocalTime.class);
        case TIMESTAMP:
            return result.getObject(column, LocalDateTime.class);
        case INTERVAL:
            Object interval = result.getObject(column);
            if (interval == null) {
                return null;
            }
            if (!(interval instanceof PGInterval)) {
                throw new SQLException("Unsupported RisingWave interval value");
            }
            PGInterval pg = (PGInterval) interval;
            // Integer components preserve interval precision without floating-point comparison.
            return List.of(pg.getYears() * 12L + pg.getMonths(), (long) pg.getDays(),
                    ((pg.getHours() * 60L + pg.getMinutes()) * 60L + pg.getWholeSeconds()) * 1000000L
                            + pg.getMicroSeconds());
        case NUMBER_ARRAY:
        case BOOLEAN_ARRAY:
        case TEXT_ARRAY:
            Array array = result.getArray(column);
            if (array == null) {
                return null;
            }
            try {
                Object elements = array.getArray();
                if (!(elements instanceof Object[])) {
                    throw new SQLException("Unsupported RisingWave array representation");
                }
                List<Object> values = new ArrayList<>();
                for (Object element : (Object[]) elements) {
                    if (element == null) {
                        values.add(null);
                    } else if (type == Type.NUMBER_ARRAY && (element instanceof Integer || element instanceof Long)) {
                        values.add(BigDecimal.valueOf(((Number) element).longValue()).stripTrailingZeros());
                    } else if (type == Type.BOOLEAN_ARRAY && element instanceof Boolean
                            || type == Type.TEXT_ARRAY && element instanceof String) {
                        values.add(element);
                    } else {
                        throw new SQLException("Unsupported RisingWave array element");
                    }
                }
                return Collections.unmodifiableList(values);
            } finally {
                array.free();
            }
        default:
            throw new AssertionError(type);
        }
    }

    public static RisingWaveRows execute(String sql, ExpectedErrors errors, RisingWaveGlobalState state)
            throws SQLException {
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(sql);
            try {
                state.getLogger().getCurrentFileWriter().flush();
            } catch (IOException failure) {
                throw new AssertionError("Cannot log RisingWave query", failure);
            }
        }
        // SQLancerResultSet does not expose the metadata needed for typed decoding.
        try (Statement statement = state.getConnection().createStatement(); ResultSet result = statement.executeQuery(
                new SQLQueryAdapter(sql, errors, true, state.getOptions().canonicalizeSqlString()).getQueryString())) {
            Main.nrSuccessfulActions.incrementAndGet();
            if (result == null) {
                throw new IgnoreMeException();
            }
            return read(result);
        } catch (Exception failure) {
            Main.nrUnsuccessfulActions.incrementAndGet();
            if (failure instanceof IgnoreMeException) {
                throw (IgnoreMeException) failure;
            }
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause.getMessage() != null && errors.errorIsExpected(cause.getMessage())) {
                    throw new IgnoreMeException();
                }
            }
            throw new AssertionError(sql, failure);
        }
    }
}
