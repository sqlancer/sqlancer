package sqlancer.mysql.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLBugs;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.MySQLSchema.MySQLTable.MySQLEngine;

public class MySQLTableGenerator {
    private final StringBuilder sb = new StringBuilder();
    private final boolean allowPrimaryKey;
    private boolean setPrimaryKey;
    private final String tableName;
    private final Randomly r;
    private boolean tableHasNullableColumn;
    private MySQLEngine engine;
    private int keysSpecified;
    private final List<String> columns = new ArrayList<>();
    private final MySQLSchema schema;
    private final MySQLGlobalState globalState;

    public MySQLTableGenerator(MySQLGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.r = globalState.getRandomly();
        this.schema = globalState.getSchema();
        allowPrimaryKey = Randomly.getBoolean();
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(MySQLGlobalState globalState, String tableName) {
        return new MySQLTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();

        sb.append("CREATE");
        // TODO support temporary tables in the schema
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
            sb.append("(");
            for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                appendColumn(i);
            }
            sb.append(")");
            sb.append(" ");
            appendTableOptions();
            appendPartitionOptions();
            if (engine == MySQLEngine.CSV && (tableHasNullableColumn || setPrimaryKey)) {
                if (true) { // TODO
                    // results in an error
                    throw new IgnoreMeException();
                }
            } else if (engine == MySQLEngine.ARCHIVE && (tableHasNullableColumn || keysSpecified > 1)) {
                errors.add("Too many keys specified; max 1 keys allowed");
                errors.add("Table handler doesn't support NULL in given index");
                addCommonErrors(errors);
                return new SQLQueryAdapter(sb.toString(), errors, true);
            }
            addCommonErrors(errors);
            return new SQLQueryAdapter(sb.toString(), errors, true);
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

    private enum PartitionOptions {
        HASH, KEY
    }

    private void appendPartitionOptions() {
        if (engine != MySQLEngine.INNO_DB) {
            return;
        }
        if (Randomly.getBoolean()) {
            return;
        }
        sb.append(" PARTITION BY");
        switch (Randomly.fromOptions(PartitionOptions.values())) {
        case HASH:
            if (Randomly.getBoolean()) {
                sb.append(" LINEAR");
            }
            sb.append(" HASH(");
            // TODO: consider arbitrary expressions
            // MySQLExpression expr =
            // MySQLRandomExpressionGenerator.generateRandomExpression(Collections.emptyList(),
            // null, r);
            // sb.append(MySQLVisitor.asString(expr));
            sb.append(Randomly.fromList(columns));
            sb.append(")");
            break;
        case KEY:
            if (Randomly.getBoolean()) {
                sb.append(" LINEAR");
            }
            sb.append(" KEY");
            if (Randomly.getBoolean()) {
                sb.append(" ALGORITHM=");
                sb.append(Randomly.fromOptions(1, 2));
            }
            sb.append(" (");
            sb.append(Randomly.nonEmptySubset(columns).stream().collect(Collectors.joining(", ")));
            sb.append(")");
            break;
        default:
            throw new AssertionError();
        }
    }

    private enum TableOptions {
        AUTO_INCREMENT, AVG_ROW_LENGTH, CHECKSUM, COMPRESSION, DELAY_KEY_WRITE, /* ENCRYPTION, */ ENGINE, INSERT_METHOD,
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
            case ENGINE:
                // FEDERATED: java.sql.SQLSyntaxErrorException: Unknown storage engine
                // 'FEDERATED'
                // "NDB": java.sql.SQLSyntaxErrorException: Unknown storage engine 'NDB'
                // "EXAMPLE": java.sql.SQLSyntaxErrorException: Unknown storage engine 'EXAMPLE'
                // "MERGE": java.sql.SQLException: Table 't0' is read only
                String fromOptions = Randomly.fromOptions("InnoDB", "MyISAM", "MEMORY", "HEAP", "CSV", "ARCHIVE");
                this.engine = MySQLEngine.get(fromOptions);
                sb.append("ENGINE = ");
                sb.append(fromOptions);
                break;
            // case ENCRYPTION:
            // sb.append("ENCRYPTION = '");
            // sb.append(Randomly.fromOptions("Y", "N"));
            // sb.append("'");
            // break;
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

    private void appendColumn(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        appendColumnDefinition();
    }

    private enum ColumnOptions {
        NULL_OR_NOT_NULL, UNIQUE, COMMENT, COLUMN_FORMAT, STORAGE, PRIMARY_KEY
    }

    private void appendColumnOption(MySQLDataType type) {
        boolean isTextType = type == MySQLDataType.VARCHAR;
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

    private void appendColumnDefinition() {
        sb.append(" ");
        MySQLDataType randomType = MySQLDataType.getRandom(globalState);
        appendType(randomType);
        sb.append(" ");
        appendColumnOption(randomType);
    }

    private void appendType(MySQLDataType randomType) {
        switch (randomType) {
        case DECIMAL:
            sb.append("DECIMAL");
            optionallyAddPrecisionAndScale(sb);
            break;
        case INT:
            sb.append(Randomly.fromOptions("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"));
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(0, 255)); // Display width out of range for column 'c0' (max =
                // 255)
                sb.append(")");
            }
            break;
        case VARCHAR:
            sb.append(Randomly.fromOptions("VARCHAR(500)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"));
            break;
        case FLOAT:
            sb.append("FLOAT");
            optionallyAddPrecisionAndScale(sb);
            break;
        case DOUBLE:
            sb.append(Randomly.fromOptions("DOUBLE", "FLOAT"));
            optionallyAddPrecisionAndScale(sb);
            break;
        default:
            throw new AssertionError();
        }
        if (randomType.isNumeric()) {
            if (Randomly.getBoolean() && randomType != MySQLDataType.INT && !MySQLBugs.bug99127) {
                sb.append(" UNSIGNED");
            }
            if (!globalState.usesPQS() && Randomly.getBoolean()) {
                sb.append(" ZEROFILL");
            }
        }
    }

    public static void optionallyAddPrecisionAndScale(StringBuilder sb) {
        if (Randomly.getBoolean() && !MySQLBugs.bug99183) {
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
