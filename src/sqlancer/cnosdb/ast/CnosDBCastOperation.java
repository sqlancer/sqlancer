package sqlancer.cnosdb.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.cnosdb.CnosDBCompoundDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBCastOperation implements CnosDBExpression {

    private final CnosDBExpression expression;
    private final CnosDBCompoundDataType type;

    public CnosDBCastOperation(CnosDBExpression expression, CnosDBCompoundDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    public static List<CnosDBDataType> canCastTo(CnosDBDataType dataType) {
        List<CnosDBDataType> options = new ArrayList<>(Arrays.asList(CnosDBDataType.values()));

        switch (dataType) {
        case UINT:
        case BOOLEAN:
        case DOUBLE:
            options.remove(CnosDBDataType.TIMESTAMP);
            break;
        case TIMESTAMP:
            options.remove(CnosDBDataType.BOOLEAN);
            options.remove(CnosDBDataType.UINT);
            options.remove(CnosDBDataType.DOUBLE);
            break;
        default:
            break;
        }
        return options;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return type.getDataType();
    }

    public CnosDBExpression getExpression() {
        return expression;
    }

    public CnosDBDataType getType() {
        return type.getDataType();
    }

    public CnosDBCompoundDataType getCompoundType() {
        return type;
    }

}
