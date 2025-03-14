package sqlancer.mysql.ast;  



public class MySQLBooleanConstant implements MySQLExpression { // ✅ Implement MySQLExpression
    private final boolean value;

    public MySQLBooleanConstant(boolean value) {
        this.value = value;
    }

    public boolean getValue() {
        return value;
    }

    public String toString() { 
        return value ? "TRUE" : "FALSE";
    }
}
