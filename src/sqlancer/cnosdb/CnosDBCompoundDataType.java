package sqlancer.cnosdb;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public final class CnosDBCompoundDataType {

    private final CnosDBDataType dataType;

    private CnosDBCompoundDataType(CnosDBDataType dataType) {
        this.dataType = dataType;
    }

    public static CnosDBCompoundDataType create(CnosDBDataType type) {
        return new CnosDBCompoundDataType(type);
    }

    public CnosDBDataType getDataType() {
        return dataType;
    }
}
