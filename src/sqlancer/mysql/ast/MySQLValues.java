package sqlancer.mysql.ast;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLValues implements MySQLExpression {

    private final LinkedHashMap<MySQLColumn, List<MySQLConstant>> values;

    public MySQLValues(LinkedHashMap<MySQLColumn, List<MySQLConstant>> v) {
        this.values = v;
    }

    public LinkedHashMap<MySQLColumn, List<MySQLConstant>> getValues() {
        return this.values;
    }
}
