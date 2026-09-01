package sqlancer.simple.expression;

import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class TableName implements Expression {
    String tableName;

    public TableName(String tableName) {
        this.tableName = tableName;
    }

    public TableName(Generator gen) {
        this.tableName = gen.generateResponse(Signal.TABLE_NAME);
    }

    @Override
    public String print() {
        return tableName;
    }
}
