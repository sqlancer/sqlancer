package sqlancer.databend;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.newast.Node;
import sqlancer.databend.ast.DatabendExpression;

public final class DatabendExprToNode {

    private DatabendExprToNode() {

    }

    @SuppressWarnings("unchecked")
    public static Node<DatabendExpression> cast(DatabendExpression expression) {
        return (Node<DatabendExpression>) expression;
    }

    @SuppressWarnings("unchecked")
    public static List<Node<DatabendExpression>> casts(List<DatabendExpression> expressions) {
        return expressions.stream().map(e -> (Node<DatabendExpression>) e).collect(Collectors.toList());
    }

}
