package sqlancer.mysql.ast;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.mysql.MySQLSchema.MySQLCompositeDataType;;

public class MySQLResultMap implements MySQLExpression {
    private final LinkedHashMap<MySQLColumnReference, List<MySQLConstant>> DBStates;
    private final List<MySQLConstant> results;
    private final HashMap<MySQLColumnReference, MySQLCompositeDataType> columnType;
    MySQLCompositeDataType resultType;

    public MySQLResultMap(LinkedHashMap<MySQLColumnReference, List<MySQLConstant>> s, 
        HashMap<MySQLColumnReference, MySQLCompositeDataType> ct, List<MySQLConstant> r, MySQLCompositeDataType rt) {
        this.DBStates = s;
        this.results = r;
        this.resultType = rt;
        this.columnType = ct;
        if (s.get(s.keySet().iterator().next()).size() != r.size()) {
            throw new AssertionError();
        }
    }

    public LinkedHashMap<MySQLColumnReference, List<MySQLConstant>> getDbStates() {
        return this.DBStates;
    }

    public List<MySQLConstant> getResult() {
        return this.results;
    }

    public HashMap<MySQLColumnReference, MySQLCompositeDataType> getColumnType() {
        return this.columnType;
    }

    public MySQLCompositeDataType getResultType() {
        return this.resultType;
    }
}
