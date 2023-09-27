package sqlancer.presto;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.newast.Node;
import sqlancer.presto.ast.PrestoExpression;

public final class PrestoExpressionToNode {

    private PrestoExpressionToNode() {

    }

    @SuppressWarnings("unchecked")
    public static Node<PrestoExpression> cast(PrestoExpression expression) {
        return (Node<PrestoExpression>) expression;
    }

    @SuppressWarnings("unchecked")
    public static List<Node<PrestoExpression>> casts(List<PrestoExpression> expressions) {
        return expressions.stream().map(e -> (Node<PrestoExpression>) e).collect(Collectors.toList());
    }

}
