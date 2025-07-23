package sqlancer.yugabyte.ysql.gen;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public class YSQLAlterTableGenerator {

    private final YSQLTable randomTable;
    private final Randomly r;
    private final YSQLGlobalState globalState;

    public YSQLAlterTableGenerator(YSQLTable randomTable, YSQLGlobalState globalState) {
        this.randomTable = randomTable;
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public static SQLQueryAdapter create(YSQLTable randomTable, YSQLGlobalState globalState) {
        return new YSQLAlterTableGenerator(randomTable, globalState).generate();
    }

    public List<Action> getActions(ExpectedErrors errors) {
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonInsertUpdateErrors(errors);
        YSQLErrors.addCommonTableErrors(errors);
        YSQLErrors.addTransactionErrors(errors);
        errors.add("duplicate key value violates unique constraint");
        errors.add("cannot drop key column");
        errors.add("cannot drop desired object(s) because other objects depend on them");
        errors.add("invalid input syntax for");
        errors.add("cannot remove a key column");
        errors.add("it has pending trigger events");
        errors.add("could not open relation");
        errors.add("functions in index expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not accept data type");
        errors.add("does not exist for access method");
        errors.add("could not find cast from");
        errors.add("does not exist"); // TODO: investigate
        errors.add("constraints on permanent tables may reference only permanent tables");
        List<Action> action;
        if (Randomly.getBoolean()) {
            action = Randomly.nonEmptySubset(Action.values());
        } else {
            // make it more likely that the ALTER TABLE succeeds
            action = Randomly.subset(Randomly.smallNumber(), Action.values());
        }
        // Remove actions that don't make sense for the current table state
        if (randomTable.getColumns().size() == 1) {
            action.remove(Action.ALTER_TABLE_DROP_COLUMN);
            action.remove(Action.RENAME_COLUMN);
        }
        if (!randomTable.hasIndexes()) {
            action.remove(Action.ADD_TABLE_CONSTRAINT_USING_INDEX);
            action.remove(Action.CLUSTER_ON);
            action.remove(Action.REPLICA_IDENTITY);
        }
        if (randomTable.isView()) {
            // Many operations don't work on views
            action.remove(Action.ADD_COLUMN);
            action.remove(Action.ALTER_COLUMN_SET_STORAGE);
            action.remove(Action.CLUSTER_ON);
            action.remove(Action.SET_TABLESPACE);
            action.remove(Action.SET_LOGGED);
            action.remove(Action.SET_UNLOGGED);
        }
        
        // Remove YugabyteDB unsupported actions
        action.remove(Action.DISABLE_RULE);
        action.remove(Action.ENABLE_RULE);
        action.remove(Action.ENABLE_REPLICA_RULE);
        action.remove(Action.ENABLE_ALWAYS_RULE);
        action.remove(Action.ALTER_COLUMN_SET_STORAGE);
        action.remove(Action.SET_WITHOUT_CLUSTER);
        action.remove(Action.ATTACH_PARTITION);
        action.remove(Action.DETACH_PARTITION);
        action.remove(Action.SET_WITH_OIDS);
        action.remove(Action.SET_WITHOUT_OIDS);
        action.remove(Action.ALTER_COLUMN_RESTART_SEQUENCE);
        action.remove(Action.SET_SCHEMA);
        action.remove(Action.SET_TABLESPACE);
        action.remove(Action.SET_ACCESS_METHOD);
        action.remove(Action.SET_LOGGED);
        action.remove(Action.SET_UNLOGGED);
        if (action.isEmpty()) {
            throw new IgnoreMeException();
        }
        return action;
    }

    public SQLQueryAdapter generate() {
        ExpectedErrors errors = new ExpectedErrors();
        int i = 0;
        List<Action> action = getActions(errors);
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        if (Randomly.getBoolean()) {
            sb.append(" ONLY");
            errors.add("cannot use ONLY for foreign key on partitioned table");
        }
        sb.append(" ");
        sb.append(randomTable.getName());
        sb.append(" ");
        for (Action a : action) {
            if (i++ != 0) {
                sb.append(", ");
            }
            switch (a) {
            // Column operations
            case ADD_COLUMN:
                sb.append("ADD ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                if (Randomly.getBoolean()) {
                    sb.append("IF NOT EXISTS ");
                }
                String columnName = DBMSCommon.createColumnName(randomTable.getColumns().size());
                sb.append(columnName).append(" ");
                YSQLDataType dataType = YSQLDataType.getRandomType();
                YSQLCommon.appendDataType(dataType, sb, false, false, globalState.getCollates());
                if (Randomly.getBoolean()) {
                    sb.append(" COLLATE ").append(globalState.getRandomCollate());
                }
                // Add column constraints
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("NOT NULL", "NULL", "DEFAULT " + getDefaultValue(dataType)));
                }
                errors.add("already exists");
                errors.add("cannot add column with primary key constraint");
                errors.add("cannot add column with unique constraint");
                break;
                
            case ALTER_TABLE_DROP_COLUMN:
                sb.append("DROP ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                if (Randomly.getBoolean()) {
                    sb.append("IF EXISTS ");
                }
                sb.append(randomTable.getRandomColumn().getName());
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("RESTRICT", "CASCADE"));
                }
                errors.add("because other objects depend on it");
                errors.add("does not exist");
                errors.add("cannot drop column");
                errors.add("cannot drop key column");
                errors.add("cannot drop inherited column");
                errors.add("is in a primary key");
                break;
                
            case ALTER_COLUMN_TYPE:
                YSQLColumn column = randomTable.getRandomColumn();
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(column.getName()).append(" ");
                if (Randomly.getBoolean()) {
                    sb.append("SET DATA ");
                }
                sb.append("TYPE ");
                YSQLDataType newType = YSQLDataType.getRandomType();
                YSQLCommon.appendDataType(newType, sb, false, false, globalState.getCollates());
                if (Randomly.getBoolean()) {
                    sb.append(" USING ").append(column.getName()).append("::text::").append(getTypeName(newType));
                }
                errors.add("cannot alter type of a column used by");
                errors.add("cannot cast");
                errors.add("out of range");
                break;
                
            case ALTER_COLUMN_SET_DEFAULT:
                column = randomTable.getRandomColumn();
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(column.getName()).append(" SET DEFAULT ");
                sb.append(getDefaultValue(column.getType()));
                errors.add("cannot use column reference in DEFAULT expression");
                break;
                
            case ALTER_COLUMN_DROP_DEFAULT:
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(randomTable.getRandomColumn().getName()).append(" DROP DEFAULT");
                break;
                
            case ALTER_COLUMN_SET_NOT_NULL:
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(randomTable.getRandomColumn().getName()).append(" SET NOT NULL");
                errors.add("contains null values");
                errors.add("column is already NOT NULL");
                break;
                
            case ALTER_COLUMN_DROP_NOT_NULL:
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(randomTable.getRandomColumn().getName()).append(" DROP NOT NULL");
                errors.add("is part of a primary key");
                break;
                
            case ALTER_COLUMN_SET_STATISTICS:
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(randomTable.getRandomColumn().getName()).append(" SET STATISTICS ");
                sb.append(Randomly.getNotCachedInteger(-1, 10000));
                break;
                
            case ALTER_COLUMN_SET_STORAGE:
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(randomTable.getRandomColumn().getName()).append(" SET STORAGE ");
                sb.append(Randomly.fromOptions("PLAIN", "EXTERNAL", "EXTENDED", "MAIN"));
                errors.add("can only have storage PLAIN");
                errors.add("ALTER action ALTER COLUMN ... SET STORAGE not supported yet");
                break;
                
            case ALTER_COLUMN_ADD_GENERATED:
                column = randomTable.getRandomColumn();
                if (column.getType() == YSQLDataType.INT || column.getType() == YSQLDataType.BIGINT) {
                    sb.append("ALTER ");
                    if (Randomly.getBoolean()) {
                        sb.append("COLUMN ");
                    }
                    sb.append(column.getName()).append(" ADD GENERATED ");
                    sb.append(Randomly.fromOptions("ALWAYS", "BY DEFAULT")).append(" AS IDENTITY");
                    if (Randomly.getBoolean()) {
                        sb.append(" (START WITH ").append(Randomly.getNotCachedInteger(1, 100)).append(")");
                    }
                    errors.add("column is already an identity column");
                    errors.add("identity column type must be smallint, integer, or bigint");
                } else {
                    throw new IgnoreMeException();
                }
                break;
                
            case ALTER_COLUMN_DROP_IDENTITY:
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(randomTable.getRandomColumn().getName()).append(" DROP IDENTITY");
                if (Randomly.getBoolean()) {
                    sb.append(" IF EXISTS");
                }
                errors.add("column is not an identity column");
                errors.add("is not an identity column");
                errors.add("is not an identity column");
                break;
                
            // Constraint operations
            case ADD_TABLE_CONSTRAINT:
                sb.append("ADD ");
                sb.append("CONSTRAINT ").append("c_").append(r.getAlphabeticChar()).append(" ");
                YSQLCommon.addTableConstraint(sb, randomTable, globalState, errors);
                errors.add("already exists");
                errors.add("multiple primary keys for table");
                errors.add("could not create unique index");
                errors.add("contains null values");
                errors.add("cannot cast type");
                errors.add("unsupported PRIMARY KEY constraint with partition key definition");
                errors.add("unsupported UNIQUE constraint with partition key definition");
                errors.add("insufficient columns in UNIQUE constraint definition");
                errors.add("which is part of the partition key");
                errors.add("out of range");
                errors.add("there is no unique constraint matching given keys for referenced table");
                errors.add("constraints on temporary tables may reference only temporary tables");
                errors.add("constraints on unlogged tables may reference only permanent or unlogged tables");
                errors.add("constraints on permanent tables may reference only permanent tables");
                errors.add("cannot reference partitioned table");
                errors.add("cannot be implemented");
                errors.add("violates foreign key constraint");
                errors.add("unsupported ON COMMIT and foreign key combination");
                errors.add("USING INDEX is not supported on partitioned tables");
                if (Randomly.getBoolean()) {
                    sb.append(" NOT VALID");
                    errors.add("cannot be marked NOT VALID");
                    errors.add("cannot add NOT VALID foreign key on partitioned table");
                } else {
                    errors.add("is violated by some row");
                }
                break;
                
            case ADD_TABLE_CONSTRAINT_USING_INDEX:
                sb.append("ADD ");
                sb.append("CONSTRAINT ").append("c_").append(r.getAlphabeticChar()).append(" ");
                sb.append(Randomly.fromOptions("UNIQUE", "PRIMARY KEY"));
                sb.append(" USING INDEX ");
                sb.append(randomTable.getRandomIndex().getIndexName());
                errors.add("already exists");
                errors.add("PRIMARY KEY containing column of type");
                errors.add("not valid");
                errors.add("is not a unique index");
                errors.add("is already associated with a constraint");
                errors.add("Cannot create a primary key or unique constraint using such an index");
                errors.add("multiple primary keys for table");
                errors.add("appears twice in unique constraint");
                errors.add("appears twice in primary key constraint");
                errors.add("contains null values");
                errors.add("insufficient columns in PRIMARY KEY constraint definition");
                errors.add("which is part of the partition key");
                break;
                
            case VALIDATE_CONSTRAINT:
                sb.append("VALIDATE CONSTRAINT ").append("c_").append(r.getAlphabeticChar());
                errors.add("does not exist");
                errors.add("is already validated");
                break;
                
            case DROP_CONSTRAINT:
                sb.append("DROP CONSTRAINT ");
                if (Randomly.getBoolean()) {
                    sb.append("IF EXISTS ");
                }
                sb.append("c_").append(r.getAlphabeticChar());
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("RESTRICT", "CASCADE"));
                }
                errors.add("does not exist");
                errors.add("cannot drop inherited constraint");
                break;
                
            // Table properties
            case RENAME_TO:
                sb.append("RENAME TO ").append("t_").append(r.getAlphabeticChar()).append(Randomly.getNotCachedInteger(0, 1000));
                errors.add("already exists");
                break;
                
            case RENAME_COLUMN:
                column = randomTable.getRandomColumn();
                sb.append("RENAME ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(column.getName()).append(" TO ").append("c_").append(r.getAlphabeticChar());
                errors.add("already exists");
                errors.add("cannot rename inherited column");
                break;
                
            case RENAME_CONSTRAINT:
                sb.append("RENAME CONSTRAINT ").append("c_").append(r.getAlphabeticChar());
                sb.append(" TO ").append("c_").append(r.getAlphabeticChar()).append("_new");
                errors.add("does not exist");
                errors.add("already exists");
                break;
                
            case SET_SCHEMA:
                sb.append("SET SCHEMA ").append(Randomly.fromOptions("public", "pg_temp"));
                errors.add("cannot move objects into or out of temporary schemas");
                errors.add("cannot move objects into or out of TOAST schema");
                break;
                
            // Storage parameters
            case SET_WITH:
                sb.append("SET (");
                String param = Randomly.fromOptions("fillfactor", "autovacuum_enabled", "toast_tuple_target");
                sb.append(param).append(" = ");
                switch (param) {
                case "fillfactor":
                    sb.append(Randomly.getNotCachedInteger(10, 100));
                    break;
                case "autovacuum_enabled":
                    sb.append(Randomly.fromOptions("true", "false"));
                    break;
                case "toast_tuple_target":
                    sb.append(Randomly.getNotCachedInteger(128, 8160));
                    break;
                }
                sb.append(")");
                errors.add("unrecognized parameter");
                break;
                
            case RESET_WITH:
                sb.append("RESET (");
                sb.append(Randomly.fromOptions("fillfactor", "autovacuum_enabled", "toast_tuple_target"));
                sb.append(")");
                errors.add("unrecognized parameter");
                break;
                
            // Clustering
            case CLUSTER_ON:
                sb.append("CLUSTER ON ").append(randomTable.getRandomIndex().getIndexName());
                errors.add("cannot cluster on");
                errors.add("cannot mark index clustered");
                break;
                
            case SET_WITHOUT_CLUSTER:
                sb.append("SET WITHOUT CLUSTER");
                errors.add("ALTER action SET WITHOUT CLUSTER not supported yet");
                break;
                
            // Logging
            case SET_LOGGED:
                sb.append("SET LOGGED");
                errors.add("cannot change LOGGED status of table");
                break;
                
            case SET_UNLOGGED:
                sb.append("SET UNLOGGED");
                errors.add("cannot change UNLOGGED status of table");
                break;
                
            // Replica identity
            case REPLICA_IDENTITY:
                sb.append("REPLICA IDENTITY ");
                if (Randomly.getBoolean() && randomTable.hasIndexes()) {
                    sb.append("USING INDEX ").append(randomTable.getRandomIndex().getIndexName());
                    errors.add("cannot use non-unique index");
                    errors.add("index contains expression columns");
                } else {
                    sb.append(Randomly.fromOptions("DEFAULT", "FULL", "NOTHING"));
                }
                break;
                
            // Row level security
            case DISABLE_ROW_LEVEL_SECURITY:
                sb.append("DISABLE ROW LEVEL SECURITY");
                break;
                
            case ENABLE_ROW_LEVEL_SECURITY:
                sb.append("ENABLE ROW LEVEL SECURITY");
                break;
                
            case FORCE_ROW_LEVEL_SECURITY:
                sb.append("FORCE ROW LEVEL SECURITY");
                break;
                
            case NO_FORCE_ROW_LEVEL_SECURITY:
                sb.append("NO FORCE ROW LEVEL SECURITY");
                break;
                
            // Triggers
            case DISABLE_TRIGGER:
                sb.append("DISABLE TRIGGER ");
                sb.append(Randomly.fromOptions("trigger_" + r.getAlphabeticChar(), "ALL", "USER"));
                errors.add("does not exist");
                break;
                
            case ENABLE_TRIGGER:
                sb.append("ENABLE TRIGGER ");
                sb.append(Randomly.fromOptions("trigger_" + r.getAlphabeticChar(), "ALL", "USER"));
                errors.add("does not exist");
                break;
                
            case ENABLE_REPLICA_TRIGGER:
                sb.append("ENABLE REPLICA TRIGGER trigger_").append(r.getAlphabeticChar());
                errors.add("does not exist");
                break;
                
            case ENABLE_ALWAYS_TRIGGER:
                sb.append("ENABLE ALWAYS TRIGGER trigger_").append(r.getAlphabeticChar());
                errors.add("does not exist");
                break;
                
            // Rules
            case DISABLE_RULE:
                sb.append("DISABLE RULE rule_").append(r.getAlphabeticChar());
                errors.add("does not exist");
                errors.add("ALTER action DISABLE RULE not supported yet");
                break;
                
            case ENABLE_RULE:
                sb.append("ENABLE RULE ");
                sb.append(Randomly.fromOptions("rule_" + r.getAlphabeticChar(), "ALL", "USER"));
                errors.add("does not exist");
                errors.add("ALTER action ENABLE RULE not supported yet");
                break;
                
            case ENABLE_REPLICA_RULE:
                sb.append("ENABLE REPLICA RULE rule_").append(r.getAlphabeticChar());
                errors.add("does not exist");
                errors.add("ALTER action ENABLE REPLICA RULE not supported yet");
                break;
                
            case ENABLE_ALWAYS_RULE:
                sb.append("ENABLE ALWAYS RULE rule_").append(r.getAlphabeticChar());
                errors.add("does not exist");
                errors.add("ALTER action ENABLE ALWAYS RULE not supported yet");
                break;
                
            // Inheritance and partitioning
            case INHERIT:
                sb.append("INHERIT ");
                if (globalState.getSchema().getDatabaseTables().size() > 1) {
                    YSQLTable parentTable = globalState.getSchema().getRandomTable(t -> t != randomTable);
                    sb.append(parentTable.getName());
                } else {
                    sb.append("parent_table");
                }
                errors.add("cannot inherit from partitioned table");
                errors.add("circular inheritance not allowed");
                errors.add("cannot inherit from temporary relation");
                errors.add("already inherits from table");
                errors.add("cannot change inheritance of partitioned table");
                errors.add("cannot inherit to temporary relation");
                break;
                
            case NO_INHERIT:
                sb.append("NO INHERIT ");
                if (globalState.getSchema().getDatabaseTables().size() > 1) {
                    YSQLTable parentTable = globalState.getSchema().getRandomTable(t -> t != randomTable);
                    sb.append(parentTable.getName());
                } else {
                    sb.append("parent_table");
                }
                errors.add("does not inherit from table");
                errors.add("cannot change inheritance of partitioned table");
                errors.add("is not a parent of relation");
                break;
                
            case ATTACH_PARTITION:
                sb.append("ATTACH PARTITION ");
                if (globalState.getSchema().getDatabaseTables().size() > 1) {
                    YSQLTable partitionTable = globalState.getSchema().getRandomTable(t -> t != randomTable);
                    sb.append(partitionTable.getName());
                } else {
                    sb.append("partition_table");
                }
                sb.append(" ");
                if (Randomly.getBoolean()) {
                    sb.append("FOR VALUES IN (");
                    for (int j = 0; j < Randomly.smallNumber() + 1; j++) {
                        if (j > 0) sb.append(", ");
                        sb.append(Randomly.getNotCachedInteger(0, 1000));
                    }
                    sb.append(")");
                } else if (Randomly.getBoolean()) {
                    sb.append("FOR VALUES FROM (");
                    int from = (int) Randomly.getNotCachedInteger(0, 500);
                    sb.append(from).append(") TO (").append(from + (int) Randomly.getNotCachedInteger(1, 500)).append(")");
                } else {
                    sb.append("DEFAULT");
                }
                errors.add("is not partitioned");
                errors.add("is already a partition");
                errors.add("cannot attach a typed table as partition");
                errors.add("cannot attach inheritance child as partition");
                errors.add("cannot attach inheritance parent as partition");
                errors.add("already has a default partition");
                errors.add("partition constraint is violated by some row");
                errors.add("cannot attach as partition of temporary relation");
                errors.add("cannot attach temporary relation as partition");
                errors.add("table being attached contains an identity column");
                errors.add("table being attached contains a generated column");
                break;
                
            case DETACH_PARTITION:
                sb.append("DETACH PARTITION ");
                if (globalState.getSchema().getDatabaseTables().size() > 1) {
                    YSQLTable partitionTable = globalState.getSchema().getRandomTable(t -> t != randomTable);
                    sb.append(partitionTable.getName());
                } else {
                    sb.append("partition_table");
                }
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("CONCURRENTLY", "FINALIZE"));
                }
                errors.add("is not partitioned");
                errors.add("is not a partition of table");
                errors.add("cannot detach partitions concurrently when a default partition exists");
                errors.add("could not obtain lock on relation");
                break;
                
            // Table properties continued
            case SET_TABLESPACE:
                sb.append("SET TABLESPACE ");
                sb.append(Randomly.fromOptions("pg_default", "pg_global"));
                errors.add("cannot move into reserved tablespace");
                errors.add("cannot move temporary tables of other sessions");
                errors.add("cannot be moved into tablespace");
                break;
                
            // Access method
            case SET_ACCESS_METHOD:
                sb.append("SET ACCESS METHOD ");
                sb.append(Randomly.fromOptions("heap", "heap2"));
                errors.add("does not exist");
                errors.add("is not a table access method");
                break;
                
            // OIDs
            case SET_WITH_OIDS:
                sb.append("SET WITH OIDS");
                errors.add("is not supported");
                errors.add("cannot add OIDs to a partitioned table");
                break;
                
            case SET_WITHOUT_OIDS:
                sb.append("SET WITHOUT OIDS");
                errors.add("is not supported");
                break;
                
            // Identity and sequences
            case ALTER_COLUMN_RESTART_SEQUENCE:
                column = randomTable.getRandomColumn();
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(column.getName()).append(" RESTART");
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    if (Randomly.getBoolean()) {
                        sb.append("WITH ");
                    }
                    sb.append(Randomly.getNotCachedInteger(1, 1000));
                }
                errors.add("is not an identity column");
                break;
                
            case ALTER_COLUMN_SET_GENERATED:
                column = randomTable.getRandomColumn();
                sb.append("ALTER ");
                if (Randomly.getBoolean()) {
                    sb.append("COLUMN ");
                }
                sb.append(column.getName()).append(" SET GENERATED ");
                sb.append(Randomly.fromOptions("ALWAYS", "BY DEFAULT"));
                errors.add("is not an identity column");
                break;
                
            default:
                throw new AssertionError(a);
            }
        }
        
        // Add error for syntax issues when combining certain actions
        if (action.size() > 1) {
            errors.add("syntax error at or near");
        }
        
        // Add general ALTER TABLE errors
        errors.add("this ALTER TABLE command is not yet supported");
        errors.add("child table");

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }
    
    private String getDefaultValue(YSQLDataType type) {
        switch (type) {
        case BOOLEAN:
            return Randomly.fromOptions("true", "false");
        case INT:
        case SMALLINT:
        case BIGINT:
            return String.valueOf(r.getInteger());
        case REAL:
        case DOUBLE_PRECISION:
        case FLOAT:
            return String.valueOf(r.getDouble());
        case TEXT:
        case VARCHAR:
        case CHAR:
            return "'" + r.getString() + "'";
        case DATE:
            return "'2024-01-01'";
        case TIMESTAMP:
        case TIMESTAMPTZ:
            return "'2024-01-01 12:00:00'";
        case UUID:
            return "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'";
        case JSON:
        case JSONB:
            return "'{\"key\": \"value\"}'";
        case INT4RANGE:
            return "'[1,10)'";
        case INT8RANGE:
            return "'[1,100)'";
        case NUMRANGE:
            return "'[0.0,1.0)'";
        case TSRANGE:
            return "'[2024-01-01 00:00:00,2024-01-02 00:00:00)'";
        case TSTZRANGE:
            return "'[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)'";
        case DATERANGE:
            return "'[2024-01-01,2024-01-02)'";
        case RANGE:
            // Generic range type - use int4range as default
            return "'[1,10)'";
        case CIDR:
            return "'192.168.1.0/24'";
        case INET:
            return "'192.168.1.1'";
        case MACADDR:
            return "'08:00:2b:01:02:03'";
        case POINT:
            return "'(1,2)'";
        case LINE:
            return "'{1,2,3}'";
        case LSEG:
            return "'[(0,0),(1,1)]'";
        case BOX:
            return "'((0,0),(1,1))'";
        case PATH:
            return "'[(0,0),(1,1),(2,0)]'";
        case POLYGON:
            return "'((0,0),(1,1),(1,0))'";
        case CIRCLE:
            return "'<(0,0),1>'";
        default:
            return "NULL";
        }
    }
    
    private String getTypeName(YSQLDataType type) {
        switch (type) {
        case BOOLEAN:
            return "boolean";
        case INT:
            return "integer";
        case SMALLINT:
            return "smallint";
        case BIGINT:
            return "bigint";
        case TEXT:
            return "text";
        case VARCHAR:
            return "varchar";
        case CHAR:
            return "char";
        case REAL:
            return "real";
        case DOUBLE_PRECISION:
            return "double precision";
        case NUMERIC:
            return "numeric";
        case DATE:
            return "date";
        case TIMESTAMP:
            return "timestamp";
        case TIMESTAMPTZ:
            return "timestamptz";
        case UUID:
            return "uuid";
        case JSON:
            return "json";
        case JSONB:
            return "jsonb";
        case INT4RANGE:
            return "int4range";
        case INT8RANGE:
            return "int8range";
        case NUMRANGE:
            return "numrange";
        case TSRANGE:
            return "tsrange";
        case TSTZRANGE:
            return "tstzrange";
        case DATERANGE:
            return "daterange";
        case RANGE:
            return "anyrange";
        case CIDR:
            return "cidr";
        case INET:
            return "inet";
        case MACADDR:
            return "macaddr";
        case POINT:
            return "point";
        case LINE:
            return "line";
        case LSEG:
            return "lseg";
        case BOX:
            return "box";
        case PATH:
            return "path";
        case POLYGON:
            return "polygon";
        case CIRCLE:
            return "circle";
        case BYTEA:
            return "bytea";
        case BIT:
            return "bit";
        case FLOAT:
            return "float";
        case TIME:
            return "time";
        case INTERVAL:
            return "interval";
        case MONEY:
            return "money";
        default:
            return "text";
        }
    }

    protected enum Action {
        // Column operations
        ADD_COLUMN, // ADD [ COLUMN ] [ IF NOT EXISTS ] column data_type [ COLLATE collation ] [ column_constraint [ ... ] ]
        ALTER_TABLE_DROP_COLUMN, // DROP [ COLUMN ] [ IF EXISTS ] column [ RESTRICT | CASCADE ]
        ALTER_COLUMN_TYPE, // ALTER [ COLUMN ] column [ SET DATA ] TYPE data_type [ COLLATE collation ] [ USING expression ]
        ALTER_COLUMN_SET_DEFAULT, // ALTER [ COLUMN ] column SET DEFAULT expression
        ALTER_COLUMN_DROP_DEFAULT, // ALTER [ COLUMN ] column DROP DEFAULT
        ALTER_COLUMN_SET_NOT_NULL, // ALTER [ COLUMN ] column SET NOT NULL
        ALTER_COLUMN_DROP_NOT_NULL, // ALTER [ COLUMN ] column DROP NOT NULL
        ALTER_COLUMN_SET_STATISTICS, // ALTER [ COLUMN ] column SET STATISTICS integer
        ALTER_COLUMN_SET_STORAGE, // ALTER [ COLUMN ] column SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
        ALTER_COLUMN_ADD_GENERATED, // ALTER [ COLUMN ] column ADD GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( sequence_options ) ]
        ALTER_COLUMN_SET_GENERATED, // ALTER [ COLUMN ] column SET GENERATED { ALWAYS | BY DEFAULT } | SET sequence_option | RESTART [ [ WITH ] restart ]
        ALTER_COLUMN_DROP_IDENTITY, // ALTER [ COLUMN ] column DROP IDENTITY [ IF EXISTS ]
        
        // Constraint operations
        ADD_TABLE_CONSTRAINT, // ADD table_constraint [ NOT VALID ]
        ADD_TABLE_CONSTRAINT_USING_INDEX, // ADD table_constraint_using_index
        VALIDATE_CONSTRAINT, // VALIDATE CONSTRAINT constraint_name
        DROP_CONSTRAINT, // DROP CONSTRAINT [ IF EXISTS ] constraint_name [ RESTRICT | CASCADE ]
        
        // Table properties
        RENAME_TO, // RENAME TO new_name
        RENAME_COLUMN, // RENAME [ COLUMN ] column TO new_column
        RENAME_CONSTRAINT, // RENAME CONSTRAINT constraint_name TO new_constraint_name
        SET_SCHEMA, // SET SCHEMA new_schema
        SET_TABLESPACE, // SET TABLESPACE new_tablespace
        
        // Storage parameters
        SET_WITH, // SET ( storage_parameter [= value] [, ... ] )
        RESET_WITH, // RESET ( storage_parameter [, ... ] )
        
        // Inheritance and partitioning
        INHERIT, // INHERIT parent_table
        NO_INHERIT, // NO INHERIT parent_table
        ATTACH_PARTITION, // ATTACH PARTITION partition_name { FOR VALUES partition_bound_spec | DEFAULT }
        DETACH_PARTITION, // DETACH PARTITION partition_name [ CONCURRENTLY | FINALIZE ]
        
        // Row level security
        DISABLE_ROW_LEVEL_SECURITY, // DISABLE ROW LEVEL SECURITY
        ENABLE_ROW_LEVEL_SECURITY, // ENABLE ROW LEVEL SECURITY
        FORCE_ROW_LEVEL_SECURITY, // FORCE ROW LEVEL SECURITY
        NO_FORCE_ROW_LEVEL_SECURITY, // NO FORCE ROW LEVEL SECURITY
        
        // Triggers
        DISABLE_TRIGGER, // DISABLE TRIGGER [ trigger_name | ALL | USER ]
        ENABLE_TRIGGER, // ENABLE TRIGGER [ trigger_name | ALL | USER ]
        ENABLE_REPLICA_TRIGGER, // ENABLE REPLICA TRIGGER trigger_name
        ENABLE_ALWAYS_TRIGGER, // ENABLE ALWAYS TRIGGER trigger_name
        
        // Rules
        DISABLE_RULE, // DISABLE RULE rewrite_rule_name
        ENABLE_RULE, // ENABLE RULE rewrite_rule_name
        ENABLE_REPLICA_RULE, // ENABLE REPLICA RULE rewrite_rule_name
        ENABLE_ALWAYS_RULE, // ENABLE ALWAYS RULE rewrite_rule_name
        
        // Clustering
        CLUSTER_ON, // CLUSTER ON index_name
        SET_WITHOUT_CLUSTER, // SET WITHOUT CLUSTER
        
        // Logging
        SET_LOGGED, // SET LOGGED
        SET_UNLOGGED, // SET UNLOGGED
        
        // Access method
        SET_ACCESS_METHOD, // SET ACCESS METHOD new_access_method
        
        // OIDs
        SET_WITH_OIDS, // SET WITH OIDS
        SET_WITHOUT_OIDS, // SET WITHOUT OIDS
        
        // Identity and sequences
        ALTER_COLUMN_RESTART_SEQUENCE, // ALTER [ COLUMN ] column RESTART [ [ WITH ] restart ]
        
        // Replica identity
        REPLICA_IDENTITY, // REPLICA IDENTITY { DEFAULT | USING INDEX index_name | FULL | NOTHING }
    }

}
