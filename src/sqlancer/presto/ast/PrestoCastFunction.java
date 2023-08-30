package sqlancer.presto.ast;

import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;

public class PrestoCastFunction implements Node<PrestoExpression> {

    private final Node<PrestoExpression> expr;
    private final PrestoSchema.PrestoCompositeDataType type;

    public PrestoCastFunction(Node<PrestoExpression> expr, PrestoSchema.PrestoCompositeDataType type) {
        this.expr = expr;
        this.type = type;
    }

    public Node<PrestoExpression> getExpr() {
        return expr;
    }

    public PrestoSchema.PrestoCompositeDataType getType() {
        return type;
    }

}
