package sqlancer.questdb;

import sqlancer.Randomly;

/**
 * Refer to <a href="https://questdb.io/docs/reference/sql/datatypes/">QuestDB data types</a>.
 * <p></p>
 * Type attributes: bitSize, isNullable.
 */
public enum QuestDBDataType {
    BOOLEAN(1, false), // null becomes false
    BYTE(8, false), // null becomes 0
    SHORT(16, false), // null becomes 0
    CHAR(16),
    INT(32),
    LONG(64),
    DATE(64),
    TIMESTAMP(64),
    FLOAT(32),
    DOUBLE(64),
    STRING(-1), // variable size: 32 + 16 * n
    SYMBOL(32),

    // not strictly a type. QuestDB stores a type dependent value in case of NULL.
    NULL(-1); // variable size

    public final int bitSize; // -1 means size is variable
    public final boolean isNullable;

    QuestDBDataType(int bitSize) {
        this(bitSize, true);
    }

    QuestDBDataType(int bitSize, boolean isNullable) {
        this.bitSize = bitSize;
        this.isNullable = isNullable;
    }

    public static QuestDBDataType getNonNullRandom() {
        // NOTE: NULL is not a type, it is used as a marker only.
        QuestDBDataType[] types = QuestDBDataType.values();
        QuestDBDataType type;
        do {
            type = Randomly.fromOptions(types);
        } while (type == QuestDBDataType.NULL);
        return type;
    }
}
