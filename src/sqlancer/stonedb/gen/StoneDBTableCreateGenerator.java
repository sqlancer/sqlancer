package sqlancer.stonedb.gen;

import static sqlancer.stonedb.gen.StoneDBTableCreateGenerator.ColumnOptions.PRIMARY_KEY;
import static sqlancer.stonedb.gen.StoneDBTableCreateGenerator.ColumnOptions.UNIQUE_KEY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.Randomly.StringGenerationStrategy;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;

public class StoneDBTableCreateGenerator {
    // the name of the table to create
    private final String tableName;
    private final StoneDBSchema schema;
    // the name of the columns in the table
    private final List<String> columns = new ArrayList<>();
    private final boolean allowPrimaryKey;
    private boolean setPrimaryKey;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();
    private final Randomly r;

    public StoneDBTableCreateGenerator(StoneDBGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.schema = globalState.getSchema();
        allowPrimaryKey = Randomly.getBoolean();
        this.r = globalState.getRandomly();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState, String tableName) {
        return new StoneDBTableCreateGenerator(globalState, tableName).getQuery();
    }

    public SQLQueryAdapter getQuery() {
        sb.append(Randomly.fromOptions("CREATE TABLE "/* , "CREATE TEMPORARY TABLE " */));
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName);
        // ues link statement
        if (Randomly.getBoolean() && !schema.getDatabaseTables().isEmpty()) {
            sb.append(" LIKE ");
            sb.append(schema.getRandomTable().getName());
        } else {
            appendColumns();
            if (Randomly.getBoolean()) {
                sb.append(" ");
                appendTableOptions();
            }
        }
        addExpectedErrors();
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void addExpectedErrors() {
        // java.sql.SQLSyntaxErrorException: BLOB/TEXT column 'c0' used in key specification without a key length
        errors.add("used in key specification without a key length");
        // java.sql.SQLException: Tianmu engine does not support unique index.
        errors.add("Tianmu engine does not support unique index");
        // java.sql.SQLException: BLOB column 'c0' can't be used in key specification with the used table type
        errors.add("can't be used in key specification with the used table type");
        // java.sql.SQLSyntaxErrorException: Specified key was too long; max key length is 3072 bytes
        errors.add("Specified key was too long; max key length is 3072 bytes");
        // java.sql.SQLSyntaxErrorException: Column length too big for column 'c1' (max = 16383); use BLOB or TEXT
        // instead
        errors.add("Column length too big for column");
        // BLOB/TEXT column 'c0' used in key specification without a key length
        errors.addRegex(Pattern.compile("BLOB/TEXT column 'c.*' used in key specification without a key length"));
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

    private void appendColumns() {
        sb.append("(");
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumn(i);
        }
        sb.append(")");
    }

    private void appendColumn(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        appendColumnDefinition();
    }

    private void appendColumnDefinition() {
        sb.append(" ");
        StoneDBDataType randomType = StoneDBDataType.getRandomWithoutNull();
        appendType(randomType);
        appendColumnOption(randomType);
    }

    protected enum ColumnOptions {
        NULL_OR_NOT_NULL, PRIMARY_KEY, UNIQUE_KEY, COMMENT, COLUMN_FORMAT, STORAGE
    }

    private void appendColumnOption(StoneDBDataType type) {
        boolean isTextType = type == StoneDBDataType.VARCHAR;
        boolean isNull = false;
        boolean columnHasPrimaryKey = false;
        List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
        // boolean tableHasNullableColumn;
        // if (!columnOptions.contains(ColumnOptions.NULL_OR_NOT_NULL)) {
        // tableHasNullableColumn = true;
        // }
        // only use one key, unique key or primary key, but not both
        if (columnOptions.contains(PRIMARY_KEY) && columnOptions.contains(UNIQUE_KEY)) {
            columnOptions.remove(Randomly.fromOptions(PRIMARY_KEY, UNIQUE_KEY));
        }
        if (isTextType) {
            // TODO: restriction due to the limited key length
            columnOptions.remove(PRIMARY_KEY);
            columnOptions.remove(UNIQUE_KEY);
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
                    // tableHasNullableColumn = true;
                    isNull = true;
                } else {
                    sb.append("NOT NULL");
                }
                break;
            case UNIQUE_KEY:
                sb.append("UNIQUE");
                if (Randomly.getBoolean()) {
                    sb.append(" KEY");
                }
                break;
            case COMMENT:
                StringGenerationStrategy strategy = Randomly.StringGenerationStrategy.ALPHANUMERIC;
                sb.append(String.format("COMMENT '%s' ", strategy.getString(r)));
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

    private void appendType(StoneDBDataType randomType) {
        sb.append(StoneDBDataType.getTypeAndValue(randomType));
    }
}
