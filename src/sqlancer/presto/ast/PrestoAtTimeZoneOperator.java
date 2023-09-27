package sqlancer.presto.ast;

import sqlancer.common.ast.newast.Node;

public class PrestoAtTimeZoneOperator implements Node<PrestoExpression> {

    private final Node<PrestoExpression> expr;
    private final Node<PrestoExpression> timeZone;

    public PrestoAtTimeZoneOperator(Node<PrestoExpression> expr, Node<PrestoExpression> timeZone) {
        this.expr = expr;
        this.timeZone = timeZone;
    }

    public Node<PrestoExpression> getExpr() {
        return expr;
    }

    public Node<PrestoExpression> getTimeZone() {
        return timeZone;
    }
}
