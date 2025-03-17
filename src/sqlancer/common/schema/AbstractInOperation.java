package sqlancer.common.schema;

import java.util.List;

public interface AbstractInOperation<T> {
    T getExpr();

    boolean isTrue();

    List<T> getListElements();
}
