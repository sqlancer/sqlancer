package sqlancer.oceanbase.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;

public class OceanBaseTableGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final boolean allowPrimaryKey;
    private boolean setPrimaryKey;
    private final String tableName;
    private final Randomly r;
    private int columnId;
    private final List<String> columns = new ArrayList<>();
    private final OceanBaseSchema schema;
    private final OceanBaseGlobalState globalState;

    public OceanBaseTableGenerator(OceanBaseGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.r = globalState.getRandomly();
        this.schema = globalState.getSchema();
        allowPrimaryKey = Randomly.getBoolean();
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(OceanBaseGlobalState globalState, String tableName) {
        return new OceanBaseTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
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
            sb.append("(");
            for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                appendColumn();
            }
            sb.append(")");
            sb.append(" ");
            appendTableOptions();
            appendPartitionOptions();
            addCommonErrors(errors);
            return new SQLQueryAdapter(sb.toString(), errors, true);
        }

    }

    private void addCommonErrors(ExpectedErrors list) {
        list.add("doesn't have this option");
        list.add("must include all columns");
        list.add("not allowed type for this type of partitioning");
        list.add("doesn't support BLOB/TEXT columns");
        list.add("A BLOB field is not allowed in partition function");
        list.add("Too many keys specified; max 1 keys allowed");
        list.add("The total length of the partitioning fields is too large");
    }

    private enum PartitionOptions {
        HASH, KEY
    }

    private void appendPartitionOptions() {
        sb.append(" PARTITION BY");
        switch (Randomly.fromOptions(PartitionOptions.values())) {
        case HASH:
            sb.append(" HASH(");
            sb.append(Randomly.fromList(columns));
            sb.append(")");
            sb.append(" partitions ");
            sb.append(r.getInteger(1, 20));
            break;
        case KEY:
            sb.append(" KEY");
            sb.append(" (");
            sb.append(Randomly.nonEmptySubset(columns).stream().collect(Collectors.joining(", ")));
            sb.append(")");
            break;
        default:
            throw new AssertionError();
        }
    }

    private enum TableOptions {
        BS, BLOOM, AUTO_INCREMENT;

        public static List<TableOptions> getRandomTableOptions() {
            List<TableOptions> options;
            if (Randomly.getBooleanWithSmallProbability()) {
                options = Randomly.subset(TableOptions.values());
            } else {
                if (Randomly.getBoolean()) {
                    options = Collections.emptyList();
                } else {
                    options = Randomly.nonEmptySubset(Arrays.asList(TableOptions.values()), 0);
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
                sb.append("AUTO_INCREMENT = " + r.getPositiveInteger());
                break;
            case BLOOM:
                sb.append("USE_BLOOM_FILTER = ");
                if (Randomly.getBoolean()) {
                    sb.append(" FALSE ");
                } else {
                    sb.append(" true ");
                }
                break;
            case BS:
                sb.append(" BLOCK_SIZE = ");
                if (Randomly.getBoolean()) {
                    sb.append(" 16384 ");
                } else {
                    sb.append(" 32768 ");
                }
                break;
            default:
                throw new AssertionError(o);
            }
        }
    }

    private void appendColumn() {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        appendColumnDefinition();
        columnId++;
    }

    private enum ColumnOptions {
        NULL_OR_NOT_NULL, UNIQUE, COMMENT, PRIMARY_KEY
    }

    private void appendColumnDefinition() {
        sb.append(" ");

        OceanBaseDataType randomType = OceanBaseDataType.getRandom(globalState);
        boolean isTextType = randomType == OceanBaseDataType.VARCHAR;
        appendTypeString(randomType);
        sb.append(" ");
        boolean isNull = false;
        boolean columnHasPrimaryKey = false;

        List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
        if (isTextType) {
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
                sb.append(String.format("COMMENT '%s' ", "asdf"));
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

    private void appendTypeString(OceanBaseDataType randomType) {
        switch (randomType) {
        case DECIMAL:
            sb.append("DECIMAL");
            optionallyAddPrecisionAndScale(sb);
            break;
        case INT:
            sb.append(Randomly.fromOptions("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"));
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(0, 255));
                sb.append(")");
            }
            break;
        case VARCHAR:
            sb.append(Randomly.fromOptions("VARCHAR(500)"));
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
            if (Randomly.getBoolean() && randomType != OceanBaseDataType.INT) {
                sb.append(" UNSIGNED");
            }
            if (Randomly.getBoolean()) {
                sb.append(" ZEROFILL");
            }
        }
    }

    public static void optionallyAddPrecisionAndScale(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            sb.append("(");
            long m = Randomly.getNotCachedInteger(1, 53);
            sb.append(m);
            sb.append(", ");
            long nCandidate = Randomly.getNotCachedInteger(1, 30);
            long n = Math.min(nCandidate, m);
            sb.append(n);
            sb.append(")");
        }
    }

}
