package sqlancer.simple.expression;

import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class ColumnName implements Expression {
    String columnName;
    String tableName;

    public ColumnName(String columnName, String tableName) {
        this.columnName = columnName;
        this.tableName = tableName;
    }

    public ColumnName(Generator gen) {
        ColumnName col = gen.generateResponse(Signal.COLUMN_NAME);
        this.columnName = col.columnName;
        this.tableName = col.tableName;
    }

    @Override
    public String print() {
        return tableName + "." + columnName;
    }
}
