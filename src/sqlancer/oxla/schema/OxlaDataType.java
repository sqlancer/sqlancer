package sqlancer.oxla.schema;

import sqlancer.Randomly;

import java.util.Arrays;

public enum OxlaDataType {
    BOOLEAN, DATE, FLOAT32, FLOAT64, INT32, INT64, INTERVAL, JSON, TEXT, TIME, TIMESTAMP, TIMESTAMPTZ;

    @Override
    public String toString() {
        return OxlaDataType.toString(this);
    }

    public static final OxlaDataType[] AGGREGABLE = Arrays.stream(values()).filter(o -> !(o == JSON || o == TEXT)).toArray(OxlaDataType[]::new);
    public static final OxlaDataType[] ALL = values();
    public static final OxlaDataType[] ANY_TIMESTAMP = new OxlaDataType[]{TIMESTAMP, TIMESTAMPTZ};
    public static final OxlaDataType[] COMPARABLE_WITHOUT_INTERVAL = Arrays.stream(values()).filter(o -> !(o == JSON || o == INTERVAL)).toArray(OxlaDataType[]::new);
    public static final OxlaDataType[] FLOATING_POINT = new OxlaDataType[]{FLOAT32, FLOAT64};
    public static final OxlaDataType[] NUMERIC = new OxlaDataType[]{INT32, INT64, FLOAT32, FLOAT64};

    public static OxlaDataType getRandomType() {
        return Randomly.fromOptions(values());
    }

    public static OxlaDataType[] repeatedType(OxlaDataType type, int count) {
        OxlaDataType[] arr = new OxlaDataType[count];
        Arrays.fill(arr, type);
        return arr;
    }

    public static OxlaDataType fromString(String type) {
        switch (type) {
            case "bool":
            case "boolean":
                return BOOLEAN;
            case "date":
                return DATE;
            case "real":
            case "float4":
                return FLOAT32;
            case "decimal":
            case "double precision":
            case "double":
            case "float":
            case "float8":
            case "numeric":
                return FLOAT64;
            case "int":
            case "int4":
            case "integer":
                return INT32;
            case "long":
            case "bigint":
            case "int8":
                return INT64;
            case "interval":
                return INTERVAL;
            case "json":
            case "jsonb":
                return JSON;
            case "char":
            case "string":
            case "text":
            case "varchar":
                return TEXT;
            case "time with time zone":
            case "time without time zone":
            case "time":
                return TIME;
            case "timestamp":
            case "timestamp without time zone":
                return TIMESTAMP;
            case "timestamp with time zone":
            case "timestamptz":
                return TIMESTAMPTZ;
            default:
                throw new AssertionError(type);
        }
    }

    public static String toString(OxlaDataType type) {
        switch (type) {
            case BOOLEAN:
                return Randomly.fromOptions("bool", "boolean");
            case DATE:
                return "date";
            case FLOAT32:
                return Randomly.fromOptions("real", "float4");
            case FLOAT64:
                return Randomly.fromOptions("decimal", "double precision", "double", "float", "float8", "numeric");
            case INT32:
                return Randomly.fromOptions("int", "int4", "integer");
            case INT64:
                return Randomly.fromOptions("long", "bigint", "int8");
            case INTERVAL:
                return "interval";
            case JSON:
                return Randomly.fromOptions("json", "jsonb");
            case TEXT:
                return Randomly.fromOptions("char", "string", "text", "varchar");
            case TIME:
                return Randomly.fromOptions("time with time zone", "time without time zone", "time");
            case TIMESTAMP:
                return Randomly.fromOptions("timestamp", "timestamp without time zone");
            case TIMESTAMPTZ:
                return Randomly.fromOptions("timestamptz", "timestamp with time zone");
            default:
                throw new AssertionError(type);
        }
    }
}
