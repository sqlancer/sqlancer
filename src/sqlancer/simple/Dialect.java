package sqlancer.simple;

import java.util.List;

import sqlancer.simple.expression.Operation;
import sqlancer.simple.type.Type;

public interface Dialect {
    List<Operation> getOperations();

    List<Type> getTypes();
}
