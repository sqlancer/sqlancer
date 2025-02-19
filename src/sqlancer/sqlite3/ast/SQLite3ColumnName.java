package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3ColumnName implements SQLite3Expression {

    private final SQLite3Schema.SQLite3Column column;
    private final SQLite3Constant value;

    public SQLite3ColumnName(SQLite3Schema.SQLite3Column name, SQLite3Constant value) {
        this.column = name;
        this.value = value;
    }

    public SQLite3Schema.SQLite3Column getColumn() {
        return column;
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        return value;
    }

    /*
     * When an expression is a simple reference to a column of a real table (not a VIEW or subquery) then the expression
     * has the same affinity as the table column.
     */
    @Override
    public TypeAffinity getAffinity() {
        switch (column.getType()) {
        case BINARY:
            return TypeAffinity.BLOB;
        case INT:
            return TypeAffinity.INTEGER;
        case NONE:
            return TypeAffinity.NONE;
        case REAL:
            return TypeAffinity.REAL;
        case TEXT:
            return TypeAffinity.TEXT;
        default:
            throw new AssertionError(column);
        }
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getImplicitCollateSequence() {
        return column.getCollateSequence();
    }

    public static SQLite3ColumnName createDummy(String string) {
        return new SQLite3ColumnName(SQLite3Schema.SQLite3Column.createDummy(string), null);
    }

}
