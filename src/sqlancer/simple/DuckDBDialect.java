package sqlancer.simple;

import java.util.List;

import sqlancer.simple.expression.Between;
import sqlancer.simple.expression.Cast;
import sqlancer.simple.expression.ColumnName;
import sqlancer.simple.expression.Constant;
import sqlancer.simple.expression.NotBetween;
import sqlancer.simple.expression.Operation;
import sqlancer.simple.type.BigInt;
import sqlancer.simple.type.Timestamp;
import sqlancer.simple.type.Type;

public class DuckDBDialect implements Dialect {
    static List<Operation> operations = List.of(new Constant.Op(), new ColumnName.Op(), new Cast.Op(), new Between.Op(),
            new NotBetween.Op());

    static List<Type> types = List.of(new BigInt(), new Timestamp());

    @Override
    public List<Operation> getOperations() {
        return operations;
    }

    @Override
    public List<Type> getTypes() {
        return types;
    }
}
