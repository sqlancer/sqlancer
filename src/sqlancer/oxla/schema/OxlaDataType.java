package sqlancer.oxla.schema;

import sqlancer.Randomly;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public enum OxlaDataType {
    BOOLEAN, COMPOSITE, DATE, FLOAT32, FLOAT64, INT32, INT64, INTERVAL, JSON, TEXT, TIME, TIMESTAMP, TIMESTAMPTZ;

    public static OxlaDataType getRandomType() {
        List<OxlaDataType> dataTypes = new ArrayList<>(Arrays.asList(values()));
        // NOTE: COMPOSITE is not a basic type, and it's only in this enum, because we need to map column types when
        // querying tables and schemas.
        dataTypes.remove(COMPOSITE);
        return Randomly.fromList(dataTypes);
    }

    public static OxlaDataType fromString(String type) {
        switch (type) {
            case "bool":
            case "boolean":
                return BOOLEAN;
            case "user-defined":
                return COMPOSITE;
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
            case COMPOSITE:
                return "user-defined";
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
