package sqlancer.h2;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;

public class H2ToStringVisitor extends NewToStringVisitor<H2Expression> {

    @Override
    public void visitSpecific(Node<H2Expression> expr) {
        if (expr instanceof H2Constant) {
            visit((H2Constant) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(H2Constant constant) {
        sb.append(constant.toString());
    }

    public static String asString(Node<H2Expression> expr) {
        H2ToStringVisitor visitor = new H2ToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
