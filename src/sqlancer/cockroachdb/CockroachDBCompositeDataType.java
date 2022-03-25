package sqlancer.cockroachdb;

import sqlancer.Randomly;

/**
 * Created by Bijitashya on 03, 2022
 */
public class CockroachDBCompositeDataType {
    protected CockroachDBSchema.CockroachDBDataType dataType;
    protected int size;
    protected CockroachDBSchema.CockroachDBCompositeDataType elementType;

    public static CockroachDBSchema.CockroachDBCompositeDataType getInt(int size) {
        return new CockroachDBSchema.CockroachDBCompositeDataType(CockroachDBSchema.CockroachDBDataType.INT, size);
    }

    public static CockroachDBSchema.CockroachDBCompositeDataType getBit(int size) {
        return new CockroachDBSchema.CockroachDBCompositeDataType(CockroachDBSchema.CockroachDBDataType.BIT, size);
    }

    public static CockroachDBSchema.CockroachDBCompositeDataType getRandom() {
        CockroachDBSchema.CockroachDBDataType randomDataType = CockroachDBSchema.CockroachDBDataType.getRandom();
        return CockroachDBCompositeDataType.getRandomForType(randomDataType);
    }

    static CockroachDBSchema.CockroachDBCompositeDataType getRandomForType(CockroachDBSchema.CockroachDBDataType randomDataType) {
        if (randomDataType == CockroachDBSchema.CockroachDBDataType.INT || randomDataType == CockroachDBSchema.CockroachDBDataType.SERIAL) {
            return new CockroachDBSchema.CockroachDBCompositeDataType(randomDataType, Randomly.fromOptions(2, 4, 8));
        } else if (randomDataType == CockroachDBSchema.CockroachDBDataType.BIT) {
            return new CockroachDBSchema.CockroachDBCompositeDataType(randomDataType, (int) Randomly.getNotCachedInteger(1, 200));
        } else if (randomDataType == CockroachDBSchema.CockroachDBDataType.VARBIT) {
            return new CockroachDBSchema.CockroachDBCompositeDataType(randomDataType, (int) Randomly.getNotCachedInteger(1, 200));
        } else if (randomDataType == CockroachDBSchema.CockroachDBDataType.ARRAY) {
            return new CockroachDBSchema.CockroachDBCompositeDataType(randomDataType, CockroachDBCompositeDataType.getRandomForType(CockroachDBCompositeDataType.getArrayElementType()));
        } else {
            return new CockroachDBSchema.CockroachDBCompositeDataType(randomDataType);
        }
    }

    private static CockroachDBSchema.CockroachDBDataType getArrayElementType() {
        while (true) {
            CockroachDBSchema.CockroachDBDataType type = CockroachDBSchema.CockroachDBDataType.getRandom();
            if (type != CockroachDBSchema.CockroachDBDataType.ARRAY && type != CockroachDBSchema.CockroachDBDataType.JSONB) {
                // nested arrays are not supported:
                // https://github.com/cockroachdb/cockroach/issues/32552
                // JSONB arrays are not supported as well:
                // https://github.com/cockroachdb/cockroach/issues/23468
                return type;
            }
        }
    }

    public static CockroachDBSchema.CockroachDBCompositeDataType getVarBit(int maxSize) {
        return new CockroachDBSchema.CockroachDBCompositeDataType(CockroachDBSchema.CockroachDBDataType.VARBIT, maxSize);
    }

    public CockroachDBSchema.CockroachDBDataType getPrimitiveDataType() {
        return dataType;
    }

    public int getSize() {
        if (size == -1) {
            throw new AssertionError(this);
        }
        return size;
    }

    public boolean isString() {
        return dataType == CockroachDBSchema.CockroachDBDataType.STRING;
    }

    @Override
    public String toString() {
        switch (dataType) {
            case INT:
                switch (size) {
                    case 2:
                        return Randomly.fromOptions("INT2", "SMALLINT");
                    case 4:
                        return "INT4";
                    case 8:
                        // "INTEGER": can be affected by a session variable
                        return Randomly.fromOptions("INT8", "INT64", "BIGINT");
                    default:
                        return "INT";
                }
            case SERIAL:
                switch (size) {
                    case 2:
                        return Randomly.fromOptions("SERIAL2", "SMALLSERIAL");
                    case 4:
                        return "SERIAL4";
                    case 8:
                        return Randomly.fromOptions("SERIAL8", "BIGSERIAL");
                    default:
                        throw new AssertionError();
                }
            case BIT:
                if (size == 1 && Randomly.getBoolean()) {
                    return "BIT";
                } else {
                    return String.format("BIT(%d)", size);
                }
            case VARBIT:
                if (size == -1) {
                    return String.format("VARBIT");
                } else {
                    return String.format("VARBIT(%d)", size);
                }
            case ARRAY:
                return String.format("%s[]", elementType.toString());
            default:
                return dataType.toString();
        }
    }

    public CockroachDBSchema.CockroachDBCompositeDataType getElementType() {
        return elementType;
    }
}
