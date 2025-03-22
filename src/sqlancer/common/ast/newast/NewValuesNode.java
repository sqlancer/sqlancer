package sqlancer.common.ast.newast;

import java.util.List;

public class NewValuesNode<T> {
    private final List<T> valuesList;
    
    public NewValuesNode(List<T> valuesList) {
        this.valuesList = valuesList;
    }

    public List<T> getValues() {
        return this.valuesList;
    }
}
