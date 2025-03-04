package sqlancer.mysql.ast;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLValuesRow implements MySQLExpression {
    private final LinkedHashMap<MySQLColumn, List<MySQLConstant>> values;

    public MySQLValuesRow(LinkedHashMap<MySQLColumn, List<MySQLConstant>> values) {
        this.values = values;
    }

    public LinkedHashMap<MySQLColumn, List<MySQLConstant>> getValues() {
        return this.values;
    }
}
