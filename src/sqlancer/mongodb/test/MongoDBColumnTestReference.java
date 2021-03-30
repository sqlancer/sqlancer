package sqlancer.mongodb.test;

import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.MongoDBSchema.MongoDBColumn;
import sqlancer.mongodb.ast.MongoDBExpression;

public class MongoDBColumnTestReference implements Node<MongoDBExpression> {

    private final MongoDBColumn columnReference;
    private final boolean inMainTable;

    public MongoDBColumnTestReference(MongoDBColumn columnReference, boolean inMainTable) {
        this.columnReference = columnReference;
        this.inMainTable = inMainTable;
    }

    public String getQueryString() {
        if (inMainTable) {
            return this.columnReference.getName();
        } else {
            return "join_" + this.columnReference.getTable().getName() + "." + this.columnReference.getName();
        }
    }

    public boolean inMainTable() {
        return inMainTable;
    }

    public String getTableName() {
        return this.columnReference.getTable().getName();
    }

    public String getPlainName() {
        return this.columnReference.getName();
    }

    public MongoDBColumn getColumnReference() {
        return columnReference;
    }
}
