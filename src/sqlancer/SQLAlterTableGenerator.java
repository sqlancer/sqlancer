package sqlancer;

import java.util.List;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractRelationalTable;

public abstract class SQLAlterTableGenerator<T extends AbstractRelationalTable<?, ?, ?>, G extends ExpandedGlobalState<?, ?>, A extends Enum<A>> {

    protected final T randomTable;
    protected final G globalState;
    protected final Randomly r;

    public SQLAlterTableGenerator(T randomTable, G globalState) {
        this.randomTable = randomTable;
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public abstract List<A> getActions(ExpectedErrors errors);

    public SQLQueryAdapter generate() {
        ExpectedErrors errors = new ExpectedErrors();
        int i = 0;
        List<A> action = getActions(errors);
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        if (Randomly.getBoolean()) {
            sb.append(" ONLY");
            errors.add("cannot use ONLY for foreign key on partitioned table");
        }
        sb.append(" ");
        sb.append(randomTable.getName());
        sb.append(" ");
        for (A a : action) {
            if (i++ != 0) {
                sb.append(", ");
            }
            switch (a.name()) {
            case "ALTER_TABLE_DROP_COLUMN":
                alterTableDropColumn(sb, errors);
                break;
            case "ADD_TABLE_CONSTRAINT":
                addTableConstraint(sb, errors);
                break;
            case "ADD_TABLE_CONSTRAINT_USING_INDEX":
                addTableConstraintIndex(sb, errors);
                break;
            case "DISABLE_ROW_LEVEL_SECURITY":
                sb.append("DISABLE ROW LEVEL SECURITY");
                break;
            case "ENABLE_ROW_LEVEL_SECURITY":
                sb.append("ENABLE ROW LEVEL SECURITY");
                break;
            case "FORCE_ROW_LEVEL_SECURITY":
                sb.append("FORCE ROW LEVEL SECURITY");
                break;
            case "NO_FORCE_ROW_LEVEL_SECURITY":
                sb.append("NO FORCE ROW LEVEL SECURITY");
                break;
            case "ALTER_COLUMN_TYPE":
                alterColumnType(sb, errors, randomTable);
                break;
            case "ALTER_COLUMN_SET_DROP_DEFAULT":
                alterColumnSetDropDefault(sb, errors, randomTable);
                break;
            case "ALTER_COLUMN_SET_DROP_NULL":
                alterColumnSetDropNull(sb, errors, randomTable);
                break;
            case "ALTER_COLUMN_SET_STATISTICS":
                alterColumnSetStatistics(sb, errors, randomTable);
                break;
            case "ALTER_COLUMN_SET_ATTRIBUTE_OPTION":
                alterColumnSetAttributeOption(sb, randomTable);
                break;
            case "ALTER_COLUMN_RESET_ATTRIBUTE_OPTION":
                alterColumnResetAttributeOption(sb, randomTable);
                break;
            case "ALTER_COLUMN_SET_STORAGE":
                alterColumnSetStorage(sb, errors, randomTable);
                break;
            case "VALIDATE_CONSTRAINT":
                validateConstraint(sb, errors);
                break;
            case "CLUSTER_ON":
                clusterOn(sb, errors);
                break;
            case "SET_WITHOUT_CLUSTER":
                setWithoutCluster(sb, errors);
                break;
            case "SET_WITH_OIDS":
                setWithOIDS(sb);
                break;
            case "SET_WITHOUT_OIDS":
                setWithoutOIDS(sb);
                break;
            case "SET_LOGGED_UNLOGGED":
                setLoggedUnlogged(sb, errors);
                break;
            case "NOT_OF":
                notOf(sb, errors);
                break;
            case "OWNER_TO":
                ownerTo(sb);
                break;
            case "REPLICA_IDENTITY":
                replicaIdentity(sb, errors, randomTable);
                break;
            default:
                throw new AssertionError(a);
            }
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void alterTableDropColumn(StringBuilder sb, ExpectedErrors errors) {
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
    }

    private void addTableConstraint(StringBuilder sb, ExpectedErrors errors) {
        sb.append("ADD ");
        sb.append("CONSTRAINT ").append(r.getAlphabeticChar()).append(" ");
        // abstracted to a method
        addTableConstraintHelper(sb, errors);
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
    }

    public void addTableConstraintHelper(StringBuilder sb, ExpectedErrors errors) {
    }

    private void addTableConstraintIndex(StringBuilder sb, ExpectedErrors errors) {
        sb.append("ADD ");
        sb.append("CONSTRAINT ").append(r.getAlphabeticChar()).append(" ");
        sb.append(Randomly.fromOptions("UNIQUE", "PRIMARY KEY"));
        errors.add("already exists");
        errors.add("not valid");
        sb.append(" USING INDEX ");
        sb.append(randomTable.getRandomIndex().getIndexName());
        addTableConstraintIndexHelper(errors);
        errors.add("is not a unique index");
        errors.add("is already associated with a constraint");
        errors.add("Cannot create a primary key or unique constraint using such an index");
        errors.add("multiple primary keys for table");
        errors.add("appears twice in unique constraint");
        errors.add("appears twice in primary key constraint");
        errors.add("contains null values");
        errors.add("insufficient columns in PRIMARY KEY constraint definition");
        errors.add("which is part of the partition key");
    }

    public void addTableConstraintIndexHelper(ExpectedErrors errors) {
    }

    public void alterColumnType(StringBuilder sb, ExpectedErrors errors, T randomTable) {
    }

    public void alterColumnSetDropDefault(StringBuilder sb, ExpectedErrors errors, T randomTable) {
    }

    public void alterColumnSetDropNull(StringBuilder sb, ExpectedErrors errors, T randomTable) {
    }

    public void alterColumnSetStatistics(StringBuilder sb, ExpectedErrors errors, T randomTable) {
    }

    public void alterColumnSetAttributeOption(StringBuilder sb, T randomTable) {
    }

    public void alterColumnResetAttributeOption(StringBuilder sb, T randomTable) {
    }

    public void alterColumnSetStorage(StringBuilder sb, ExpectedErrors errors, T randomTable) {
    }

    public void validateConstraint(StringBuilder sb, ExpectedErrors errors) {
    }

    public void clusterOn(StringBuilder sb, ExpectedErrors errors) {
    }

    public void setWithoutCluster(StringBuilder sb, ExpectedErrors errors) {
    }

    public void setWithOIDS(StringBuilder sb) {
    }

    public void setWithoutOIDS(StringBuilder sb) {
    }

    public void setLoggedUnlogged(StringBuilder sb, ExpectedErrors errors) {
    }

    public void notOf(StringBuilder sb, ExpectedErrors errors) {
    }

    public void ownerTo(StringBuilder sb) {
    }

    public void replicaIdentity(StringBuilder sb, ExpectedErrors errors, T randomTable) {
    }
}
