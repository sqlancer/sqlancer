package sqlancer.sqlite3.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Options.SQLite3OracleFactory;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;

public class SQLite3ColumnBuilder {

    private boolean containsPrimaryKey;
    private boolean containsAutoIncrement;
    private final StringBuilder sb = new StringBuilder();
    private boolean conflictClauseInserted;

    private boolean allowPrimaryKey = true;
    private boolean allowUnique = true;
    private boolean allowDefaultValue = true;
    private boolean allowCheck = true;
    private boolean allowNotNull = true;

    private enum Constraints {
        NOT_NULL, PRIMARY_KEY, UNIQUE, CHECK, GENERATED_AS
    }

    public boolean isContainsAutoIncrement() {
        return containsAutoIncrement;
    }

    public boolean isConflictClauseInserted() {
        return conflictClauseInserted;
    }

    public boolean isContainsPrimaryKey() {
        return containsPrimaryKey;
    }

    public String createColumn(String columnName, SQLite3GlobalState globalState, List<SQLite3Column> columns) {
        if (globalState.getDbmsSpecificOptions().oracles == SQLite3OracleFactory.PQS
                || !globalState.getDbmsSpecificOptions().testCheckConstraints) {
            allowCheck = false;
        }
        sb.append(columnName);
        sb.append(" ");
        String dataType = Randomly.fromOptions("INT", "TEXT", "BLOB", "REAL", "INTEGER");
        sb.append(dataType);

        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<Constraints> constraints = Randomly.subset(Constraints.values());
            if (!Randomly.getBooleanWithSmallProbability()
                    || globalState.getDbmsSpecificOptions().testGeneratedColumns) {
                constraints.remove(Constraints.GENERATED_AS);
            }
            if (constraints.contains(Constraints.GENERATED_AS)) {
                allowDefaultValue = false;
                allowPrimaryKey = false;
            }
            for (Constraints c : constraints) {
                switch (c) {
                case GENERATED_AS:
                    sb.append(" GENERATED ALWAYS AS (");
                    sb.append(SQLite3Visitor.asString(new SQLite3ExpressionGenerator(globalState)
                            .deterministicOnly().setColumns(columns.stream()
                                    .filter(p -> !p.getName().contentEquals(columnName)).collect(Collectors.toList()))
                            .generateExpression()));
                    sb.append(")");
                    break;
                case PRIMARY_KEY:
                    // only one primary key is allow if not specified as table constraint
                    if (allowPrimaryKey) {
                        sb.append(" PRIMARY KEY");
                        containsPrimaryKey = true;
                        boolean hasOrdering = Randomly.getBoolean();
                        if (hasOrdering) {
                            if (Randomly.getBoolean()) {
                                sb.append(" ASC");
                            } else {
                                sb.append(" DESC");
                            }
                        }
                        if (Randomly.getBoolean()) {
                            insertOnConflictClause();
                        }
                        if (!hasOrdering && dataType.equals("INTEGER") && Randomly.getBoolean()) {
                            containsAutoIncrement = true;
                            sb.append(" AUTOINCREMENT");
                        }
                    }
                    break;
                case UNIQUE:
                    if (allowUnique) {
                        sb.append(" UNIQUE");
                        if (Randomly.getBoolean()) {
                            insertOnConflictClause();
                        }
                    }
                    break;
                case NOT_NULL:
                    if (allowNotNull) {
                        sb.append(" NOT NULL");
                        if (Randomly.getBoolean()) {
                            insertOnConflictClause();
                        }
                    }
                    break;
                case CHECK:
                    if (allowCheck) {
                        sb.append(SQLite3Common.getCheckConstraint(globalState, columns));
                    }
                    break;
                default:
                    throw new AssertionError();
                }
            }
        }
        if (allowDefaultValue && Randomly.getBooleanWithSmallProbability()) {
            sb.append(" DEFAULT ");
            sb.append(SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(globalState)));
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            String randomCollate = SQLite3Common.getRandomCollate();
            sb.append(randomCollate);
        }
        return sb.toString();
    }

    // it seems that only one conflict clause can be inserted
    private void insertOnConflictClause() {
        if (!conflictClauseInserted) {
            sb.append(" ON CONFLICT ");
            sb.append(Randomly.fromOptions("ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"));
            conflictClauseInserted = true;
        }
    }

    public SQLite3ColumnBuilder allowPrimaryKey(boolean allowPrimaryKey) {
        this.allowPrimaryKey = allowPrimaryKey;
        return this;
    }

    public SQLite3ColumnBuilder allowUnique(boolean allowUnique) {
        this.allowUnique = allowUnique;
        return this;
    }

    public SQLite3ColumnBuilder allowDefaultValue(boolean allowDefaultValue) {
        this.allowDefaultValue = allowDefaultValue;
        return this;
    }

    public SQLite3ColumnBuilder allowCheck(boolean allowCheck) {
        this.allowCheck = allowCheck;
        return this;
    }

    public SQLite3ColumnBuilder allowNotNull(boolean allowNotNull) {
        this.allowNotNull = allowNotNull;
        return this;
    }

}
