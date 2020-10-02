package sqlancer.h2;

import sqlancer.common.ast.newast.Node;
import sqlancer.h2.H2Schema.H2CompositeDataType;

public class H2CastNode implements Node<H2Expression> {

    private final Node<H2Expression> expression;
    private final H2CompositeDataType type;

    public H2CastNode(Node<H2Expression> expression, H2CompositeDataType type) {
        this.expression = expression;
        this.type = type;
    }

    public Node<H2Expression> getExpression() {
        return expression;
    }

    public H2CompositeDataType getType() {
        return type;
    }

}
