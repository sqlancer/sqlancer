package sqlancer.stonedb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.stonedb.ast.StoneDBConstant;
import sqlancer.stonedb.ast.StoneDBExpression;

public class StoneDBToStringVisitor extends NewToStringVisitor<StoneDBExpression> {
    @Override
    public void visitSpecific(Node<StoneDBExpression> expr) {
        if (expr instanceof StoneDBConstant) {
            visit((StoneDBConstant) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }
    
    private void visit(StoneDBConstant constant) {
        sb.append(constant.toString());
    }
    
    public static String asString(Node<StoneDBExpression> expr) {
        StoneDBToStringVisitor visitor = new StoneDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
