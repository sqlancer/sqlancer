package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StoneDBTableGenerator {
    private final Randomly r;
    private final boolean allowPrimaryKey;
    private boolean setPrimaryKey;
    private final StringBuilder sb = new StringBuilder();

    private final StoneDBGlobalState globalState;
    private final List<String> columns = new ArrayList<>();
    private boolean tableHasNullableColumn;
    private int keysSpecified;

    private final StoneDBSchema schema;
    private final String tableName;

    public StoneDBTableGenerator(StoneDBGlobalState globalState, String tableName) {
        this.globalState = globalState;
        this.r = globalState.getRandomly();
        allowPrimaryKey = Randomly.getBoolean();
        this.schema = globalState.getSchema();
        this.tableName = tableName;
    }


    public static SQLQueryAdapter generate(StoneDBGlobalState globalState, String tableName) {
        return new StoneDBTableGenerator(globalState, tableName).getQuery();
    }

    public SQLQueryAdapter getQuery() {
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("CREATE");
        sb.append(" TABLE");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ");
        sb.append(tableName);
        if (Randomly.getBoolean() && !schema.getDatabaseTables().isEmpty()) {
            sb.append(" LIKE ");
            sb.append(schema.getRandomTable().getName());
            return new SQLQueryAdapter(sb.toString(), true);
        } else {
            appendColumnsString();
            sb.append(" ");
            appendTableOptions();
            addCommonErrors(errors);
            return new SQLQueryAdapter(sb.toString(), errors, true);
        }
    }

    private enum TableOptions {
        AUTO_INCREMENT, AVG_ROW_LENGTH, CHECKSUM, COMPRESSION, DELAY_KEY_WRITE, /* ENCRYPTION, */ INSERT_METHOD,
        KEY_BLOCK_SIZE, MAX_ROWS, MIN_ROWS, PACK_KEYS, STATS_AUTO_RECALC, STATS_PERSISTENT, STATS_SAMPLE_PAGES;

        public static List<TableOptions> getRandomTableOptions() {
            List<TableOptions> options;
            // try to ensure that usually, only a few of these options are generated
            if (Randomly.getBooleanWithSmallProbability()) {
                options = Randomly.subset(TableOptions.values());
            } else {
                if (Randomly.getBoolean()) {
                    options = Collections.emptyList();
                } else {
                    options = Randomly.nonEmptySubset(Arrays.asList(TableOptions.values()), Randomly.smallNumber());
                }
            }
            return options;
        }
    }

    private void appendTableOptions() {
        List<TableOptions> tableOptions = TableOptions.getRandomTableOptions();
        int i = 0;
        for (TableOptions o : tableOptions) {
            if (i++ != 0) {
                sb.append(", ");
            }
            switch (o) {
                case AUTO_INCREMENT:
                    sb.append("AUTO_INCREMENT = ");
                    sb.append(r.getPositiveInteger());
                    break;
                // The valid range for avg_row_length is [0,4294967295]
                case AVG_ROW_LENGTH:
                    sb.append("AVG_ROW_LENGTH = ");
                    sb.append(r.getLong(0, 4294967295L + 1));
                    break;
                case CHECKSUM:
                    sb.append("CHECKSUM = 1");
                    break;
                case COMPRESSION:
                    sb.append("COMPRESSION = '");
                    sb.append(Randomly.fromOptions("ZLIB", "LZ4", "NONE"));
                    sb.append("'");
                    break;
                case DELAY_KEY_WRITE:
                    sb.append("DELAY_KEY_WRITE = ");
                    sb.append(Randomly.fromOptions(0, 1));
                    break;
                case INSERT_METHOD:
                    sb.append("INSERT_METHOD = ");
                    sb.append(Randomly.fromOptions("NO", "FIRST", "LAST"));
                    break;
                // The valid range for key_block_size is [0,65535]
                case KEY_BLOCK_SIZE:
                    sb.append("KEY_BLOCK_SIZE = ");
                    sb.append(r.getInteger(0, 65535 + 1));
                    break;
                case MAX_ROWS:
                    sb.append("MAX_ROWS = ");
                    sb.append(r.getLong(0, Long.MAX_VALUE));
                    break;
                case MIN_ROWS:
                    sb.append("MIN_ROWS = ");
                    sb.append(r.getLong(1, Long.MAX_VALUE));
                    break;
                case PACK_KEYS:
                    sb.append("PACK_KEYS = ");
                    sb.append(Randomly.fromOptions("1", "0", "DEFAULT"));
                    break;
                case STATS_AUTO_RECALC:
                    sb.append("STATS_AUTO_RECALC = ");
                    sb.append(Randomly.fromOptions("1", "0", "DEFAULT"));
                    break;
                case STATS_PERSISTENT:
                    sb.append("STATS_PERSISTENT = ");
                    sb.append(Randomly.fromOptions("1", "0", "DEFAULT"));
                    break;
                case STATS_SAMPLE_PAGES:
                    sb.append("STATS_SAMPLE_PAGES = ");
                    sb.append(r.getInteger(1, Short.MAX_VALUE));
                    break;
                default:
                    throw new AssertionError(o);
            }
        }
    }

    private void addCommonErrors(ExpectedErrors list) {
        list.add("The storage engine for the table doesn't support");
        list.add("doesn't have this option");
        list.add("must include all columns");
        list.add("not allowed type for this type of partitioning");
        list.add("doesn't support BLOB/TEXT columns");
        list.add("A BLOB field is not allowed in partition function");
        list.add("Too many keys specified; max 1 keys allowed");
        list.add("The total length of the partitioning fields is too large");
        list.add("Got error -1 - 'Unknown error -1' from storage engine");
    }

    private void appendColumnsString() {
        sb.append("(");
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumnString(i);
        }
        sb.append(")");
    }


    private void appendColumnString(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        appendColumnDefinitionString();
    }


    private void appendColumnDefinitionString() {
        sb.append(" ");
        StoneDBDataType randomType = StoneDBDataType.getRandomWithoutNull();
        appendTypeString(randomType);
        sb.append(" ");
        appendColumnOptionString(randomType);
    }

    private enum ColumnOptions {
        NULL_OR_NOT_NULL, UNIQUE, COMMENT, COLUMN_FORMAT, STORAGE, PRIMARY_KEY
    }

    private void appendColumnOptionString(StoneDBDataType type) {
        boolean isTextType = type == StoneDBDataType.VARCHAR;
        boolean isNull = false;
        boolean columnHasPrimaryKey = false;
        List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
        if (!columnOptions.contains(ColumnOptions.NULL_OR_NOT_NULL)) {
            tableHasNullableColumn = true;
        }
        if (isTextType) {
            // TODO: restriction due to the limited key length
            columnOptions.remove(ColumnOptions.PRIMARY_KEY);
            columnOptions.remove(ColumnOptions.UNIQUE);
        }
        for (ColumnOptions o : columnOptions) {
            sb.append(" ");
            switch (o) {
                case NULL_OR_NOT_NULL:
                    // PRIMARY KEYs cannot be NULL
                    if (!columnHasPrimaryKey) {
                        if (Randomly.getBoolean()) {
                            sb.append("NULL");
                        }
                        tableHasNullableColumn = true;
                        isNull = true;
                    } else {
                        sb.append("NOT NULL");
                    }
                    break;
                case UNIQUE:
                    sb.append("UNIQUE");
                    keysSpecified++;
                    if (Randomly.getBoolean()) {
                        sb.append(" KEY");
                    }
                    break;
                case COMMENT:
                    // TODO: generate randomly
                    sb.append(String.format("COMMENT '%s' ", "asdf"));
                    break;
                case COLUMN_FORMAT:
                    sb.append("COLUMN_FORMAT ");
                    sb.append(Randomly.fromOptions("FIXED", "DYNAMIC", "DEFAULT"));
                    break;
                case STORAGE:
                    sb.append("STORAGE ");
                    sb.append(Randomly.fromOptions("DISK", "MEMORY"));
                    break;
                case PRIMARY_KEY:
                    // PRIMARY KEYs cannot be NULL
                    if (allowPrimaryKey && !setPrimaryKey && !isNull) {
                        sb.append("PRIMARY KEY");
                        setPrimaryKey = true;
                        columnHasPrimaryKey = true;
                    }
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }

    private void appendTypeString(StoneDBDataType randomType) {
        switch (randomType) {
        }
    }
}
