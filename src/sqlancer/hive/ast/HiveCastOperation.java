package sqlancer.hive.ast;

import sqlancer.hive.HiveSchema.HiveDataType;

public class HiveCastOperation implements HiveExpression {
    
    private final HiveExpression expression;
    private final HiveDataType type;
    
    public HiveCastOperation(HiveExpression expression, HiveDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }
    
    public HiveExpression getExpression() {
        return expression;
    }
    
    public HiveDataType getType() {
        return type;
    }
}
