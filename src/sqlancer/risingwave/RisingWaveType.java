package sqlancer.risingwave;

import java.util.Locale;

public enum RisingWaveType {
    BOOLEAN("boolean"), INT("integer"), BIGINT("bigint"), DECIMAL("decimal"), TEXT("varchar"), DATE("date"),
    TIMESTAMP("timestamp"), INTERVAL("interval"), SMALLINT("smallint"), TIME("time without time zone"), REAL("real"),
    DOUBLE("double precision"), INT_ARRAY("integer[]", INT), BIGINT_ARRAY("bigint[]", BIGINT),
    BOOLEAN_ARRAY("boolean[]", BOOLEAN), TEXT_ARRAY("varchar[]", TEXT),
    ROW_INT("struct<a integer>", new Field("a", INT)),
    ROW_PAIR("struct<b bigint, p boolean>", new Field("b", BIGINT), new Field("p", BOOLEAN)),
    ROW_MIXED("struct<a integer, b varchar, c boolean>", new Field("a", INT), new Field("b", TEXT),
            new Field("c", BOOLEAN));

    private final String sql;
    private final RisingWaveType elementType;
    private final java.util.List<Field> fields;

    public static final class Field {
        public final String name;
        public final RisingWaveType type;

        Field(String name, RisingWaveType type) {
            this.name = name;
            this.type = type;
        }
    }

    RisingWaveType(String sql, Field... fields) {
        this.sql = sql;
        this.elementType = null;
        this.fields = java.util.List.of(fields);
    }

    public boolean struct() {
        return !fields.isEmpty();
    }

    public java.util.List<Field> fields() {
        return fields;
    }

    RisingWaveType(String sql) {
        this(sql, (RisingWaveType) null);
    }

    RisingWaveType(String sql, RisingWaveType elementType) {
        this.sql = sql;
        this.elementType = elementType;
        this.fields = java.util.List.of();
    }

    public boolean array() {
        return elementType != null;
    }

    public RisingWaveType elementType() {
        if (!array()) {
            throw new IllegalStateException("Not an array: " + this);
        }
        return elementType;
    }

    public String sql() {
        return sql;
    }

    public boolean numeric() {
        return this == SMALLINT || this == INT || this == BIGINT || this == DECIMAL || floatingPoint();
    }

    public boolean floatingPoint() {
        return this == REAL || this == DOUBLE;
    }

    public static RisingWaveType fromSql(String sql) {
        if (sql.toLowerCase(Locale.ROOT).startsWith("struct<")) {
            String normalized = sql.toLowerCase(Locale.ROOT).replace("character varying", "varchar").replaceAll("\\s+",
                    "");
            for (RisingWaveType type : values()) {
                if (type.struct() && normalized.equals(type.sql.replaceAll("\\s+", ""))) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unsupported struct layout: " + sql);
        }
        switch (sql.toLowerCase(Locale.ROOT)) {
        case "integer[]":
        case "_int4":
            return INT_ARRAY;
        case "bigint[]":
        case "_int8":
            return BIGINT_ARRAY;
        case "boolean[]":
        case "_bool":
            return BOOLEAN_ARRAY;
        case "character varying[]":
        case "varchar[]":
        case "text[]":
        case "_varchar":
        case "_text":
            return TEXT_ARRAY;
        case "real":
        case "float4":
            return REAL;
        case "double precision":
        case "float8":
            return DOUBLE;
        case "boolean":
            return BOOLEAN;
        case "integer":
        case "int4":
            return INT;
        case "smallint":
        case "int2":
            return SMALLINT;
        case "bigint":
            return BIGINT;
        case "numeric":
            return DECIMAL;
        case "character varying":
        case "text":
            return TEXT;
        case "date":
            return DATE;
        case "timestamp without time zone":
            return TIMESTAMP;
        case "time without time zone":
        case "time":
            return TIME;
        case "interval":
            return INTERVAL;
        default:
            throw new IllegalArgumentException("Unsupported RisingWave type: " + sql);
        }
    }
}
