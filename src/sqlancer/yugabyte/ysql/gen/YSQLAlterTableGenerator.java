package sqlancer.yugabyte.ysql.gen;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
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
        if (randomTable.getColumns().size() == 1) {
            action.remove(Action.ALTER_TABLE_DROP_COLUMN);
        }
        if (!randomTable.hasIndexes()) {
            action.remove(Action.ADD_TABLE_CONSTRAINT_USING_INDEX);
        }
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
            case ALTER_TABLE_DROP_COLUMN:
                sb.append("DROP ");
                if (Randomly.getBoolean()) {
                    sb.append(" IF EXISTS ");
                }
                sb.append(randomTable.getRandomColumn().getName());
                errors.add("because other objects depend on it");
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("RESTRICT", "CASCADE"));
                }
                errors.add("does not exist");
                errors.add("cannot drop column");
                errors.add("cannot drop key column");
                errors.add("cannot drop inherited column");
                break;
            case ADD_TABLE_CONSTRAINT:
                sb.append("ADD ");
                sb.append("CONSTRAINT ").append(r.getAlphabeticChar()).append(" ");
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
                sb.append("CONSTRAINT ").append(r.getAlphabeticChar()).append(" ");
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
            default:
                throw new AssertionError(a);
            }
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    protected enum Action {
        // ALTER_TABLE_ADD_COLUMN, // [ COLUMN ] column data_type [ COLLATE collation ] [
        // column_constraint [ ... ] ]
        ALTER_TABLE_DROP_COLUMN, // DROP [ COLUMN ] [ IF EXISTS ] column [ RESTRICT | CASCADE ]
        ADD_TABLE_CONSTRAINT, // ADD table_constraint [ NOT VALID ]
        ADD_TABLE_CONSTRAINT_USING_INDEX, // ADD table_constraint_using_index
        DISABLE_ROW_LEVEL_SECURITY, // DISABLE ROW LEVEL SECURITY
        ENABLE_ROW_LEVEL_SECURITY, // ENABLE ROW LEVEL SECURITY
        FORCE_ROW_LEVEL_SECURITY, // FORCE ROW LEVEL SECURITY
        NO_FORCE_ROW_LEVEL_SECURITY, // NO FORCE ROW LEVEL SECURITY
    }

}
