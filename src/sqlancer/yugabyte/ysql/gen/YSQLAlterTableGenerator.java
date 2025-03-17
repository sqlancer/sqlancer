package sqlancer.yugabyte.ysql.gen;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLAlterTableGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public class YSQLAlterTableGenerator
        extends SQLAlterTableGenerator<YSQLTable, YSQLGlobalState, YSQLAlterTableGenerator.Action> {
    public YSQLAlterTableGenerator(YSQLTable randomTable, YSQLGlobalState globalState) {
        super(randomTable, globalState);
    }

    public static SQLQueryAdapter create(YSQLTable randomTable, YSQLGlobalState globalState) {
        return new YSQLAlterTableGenerator(randomTable, globalState).generate();
    }

    @Override
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

    @Override
    public void addTableConstraintHelper(StringBuilder sb, ExpectedErrors errors) {
        YSQLCommon.addTableConstraint(sb, randomTable, globalState, errors);
    }

    @Override
    public void addTableConstraintIndexHelper(ExpectedErrors errors) {
        errors.add("PRIMARY KEY containing column of type");
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
