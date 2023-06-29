package sqlancer.stonedb.gen;

import static sqlancer.stonedb.gen.StoneDBTableCreateGenerator.ColumnOptions.PRIMARY_KEY;
import static sqlancer.stonedb.gen.StoneDBTableCreateGenerator.ColumnOptions.UNIQUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
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
        sb.append(Randomly.fromOptions("CREATE TABLE ", "CREATE TEMPORARY TABLE "));
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName);
        // ues link statement
        if (Randomly.getBoolean() && !schema.getDatabaseTables().isEmpty()) {
            sb.append(" LIKE ");
            sb.append(schema.getRandomTable().getName());
            return new SQLQueryAdapter(sb.toString(), true);
        } else {
            appendColumns();
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
        list.add("used in key specification without a key length");
        // java.sql.SQLSyntaxErrorException: BLOB/TEXT column 'c0' used in key specification without a key length
        list.add("Got error -1 - 'Unknown error -1' from storage engine");
        // java.sql.SQLException: Tianmu engine does not support unique index.
        list.add("Tianmu engine does not support unique index");
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
        sb.append(" ");
        appendColumnOption(randomType);
    }

    protected enum ColumnOptions {
        NULL_OR_NOT_NULL, UNIQUE, COMMENT, COLUMN_FORMAT, STORAGE, PRIMARY_KEY
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
        if (columnOptions.contains(PRIMARY_KEY) && columnOptions.contains(UNIQUE)) {
            columnOptions.remove(Randomly.fromOptions(PRIMARY_KEY, UNIQUE));
        }
        if (isTextType) {
            // TODO: restriction due to the limited key length
            columnOptions.remove(PRIMARY_KEY);
            columnOptions.remove(UNIQUE);
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
            case UNIQUE:
                sb.append("UNIQUE");
                if (Randomly.getBoolean()) {
                    sb.append(" KEY");
                }
                break;
            case COMMENT:
                sb.append(String.format("COMMENT '%s' ", new Randomly().getString()));
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
        switch (randomType) {
        case TINYINT:
            sb.append("TINYINT");
            // sb.append(r.getInteger(-128, 127));
            break;
        case SMALLINT:
            sb.append("SMALLINT");
            // sb.append(r.getInteger(-32768, 32767));
            break;
        case MEDIUMINT:
            sb.append("MEDIUMINT");
            // sb.append(r.getInteger(-8388608, 8388607));
            break;
        case INT:
            sb.append("INT");
            // sb.append(r.getInteger(-2147483647, 2147483647));
            break;
        case BIGINT:
            sb.append("BIGINT");
            // sb.append(r.getBigInteger(new BigInteger("-9223372036854775806"), new
            // BigInteger("9223372036854775807")));
            break;
        case FLOAT:
            sb.append("FLOAT");
            optionallyAddPrecisionAndScale(sb);
            break;
        case DOUBLE:
            sb.append("DOUBLE");
            optionallyAddPrecisionAndScale(sb);
            break;
        case DECIMAL:
            sb.append("DECIMAL"); // The default value is P(10,0);
            break;
        case YEAR:
            sb.append("YEAR");
            break;
        case TIME:
            sb.append("TIME");
            break;
        case DATE:
            sb.append("DATE");
            break;
        case DATETIME:
            sb.append("DATETIME");
            break;
        case TIMESTAMP:
            sb.append("TIMESTAMP");
            break;
        case CHAR:
            sb.append("CHAR(").append(Randomly.fromOptions("", new Randomly().getInteger(0, 255) + ")"));
            break;
        case VARCHAR:
            sb.append("VARCHAR(").append(Randomly.fromOptions("", new Randomly().getInteger(0, 65535) + ")"));
            break;
        case TINYTEXT:
            sb.append("TINYTEXT").append(Randomly.fromOptions("", new Randomly().getInteger(0, 255) + ")"));
            break;
        case TEXT:
            sb.append("TEXT").append(Randomly.fromOptions("", new Randomly().getInteger(0, 65535) + ")"));
            break;
        case MEDIUMTEXT:
            sb.append("MEDIUMTEXT").append(Randomly.fromOptions("", new Randomly().getInteger(0, 16777215) + ")"));
            break;
        case LONGTEXT:
            sb.append("LONGTEXT").append(Randomly.fromOptions("", new Randomly().getLong(0L, 4294967295L) + ")"));
            break;
        case BINARY:
            sb.append("BINARY");
            break;
        case VARBINARY:
            sb.append("VARBINARY");
            break;
        case TINYBLOB:
            sb.append("TINYBLOB");
            break;
        case BLOB:
            sb.append("BLOB");
            break;
        case MEDIUMBLOB:
            sb.append("MEDIUMBLOB");
            break;
        case LONGBLOB:
            sb.append("LONGBLOB");
            break;
        default:
            throw new AssertionError();
        }
    }

    public static void optionallyAddPrecisionAndScale(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            sb.append("(");
            // The maximum number of digits (M) for DECIMAL is 65
            long m = Randomly.getNotCachedInteger(1, 65);
            sb.append(m);
            sb.append(", ");
            // The maximum number of supported decimals (D) is 30
            long nCandidate = Randomly.getNotCachedInteger(1, 30);
            // For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'c0').
            long n = Math.min(nCandidate, m);
            sb.append(n);
            sb.append(")");
        }
    }
}
