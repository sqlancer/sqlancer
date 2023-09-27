package sqlancer.doris.visitor;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.newast.Node;
import sqlancer.doris.ast.DorisExpression;

public final class DorisExprToNode {

    private DorisExprToNode() {

    }

    @SuppressWarnings("unchecked")
    public static Node<DorisExpression> cast(DorisExpression expression) {
        return (Node<DorisExpression>) expression;
    }

    @SuppressWarnings("unchecked")
    public static List<Node<DorisExpression>> casts(List<DorisExpression> expressions) {
        return expressions.stream().map(e -> (Node<DorisExpression>) e).collect(Collectors.toList());
    }

}
