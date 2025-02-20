package sqlancer.clickhouse.ast;

public enum ClickHouseTypeAffinity {
    NOTHING, UINT8, UINT16, UINT32, UINT64, UINT128, INT8, INT16, INT32, INT64, INT128, FLOAT32, FLOAT64, DATE,
    DATETIME, DATETIME64, STRING, FIXEDSTRING, ENUM8, ENUM16, DECIMAL32, DECIMAL64, DECIMAL128, UUID, ARRAY, TUPLE,
    SET, INTERVAL;
    // NULLABLE, FUNCTION, AGGREGATEFUNCTION, LOWCARDINALITY;

    public boolean isNumeric() {
        return this == UINT8 || this == UINT16 || this == UINT32 || this == UINT64 || this == UINT128
                || this == INT8 || this == INT16 || this == INT32 || this == INT64 || this == INT128
                || this == FLOAT32 || this == FLOAT64;
    }
}
