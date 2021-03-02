package sqlancer.arangodb.visitor;

import sqlancer.arangodb.ast.ArangoDBConstant;
import sqlancer.arangodb.ast.ArangoDBExpression;
import sqlancer.arangodb.ast.ArangoDBSelect;
import sqlancer.arangodb.query.ArangoDBSelectQuery;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;

public abstract class ArangoDBVisitor {

    protected abstract void visit(ArangoDBSelect<ArangoDBExpression> expression);

    protected abstract void visit(ColumnReferenceNode<?, ?> expression);

    protected abstract void visit(ArangoDBConstant expression);

    protected abstract void visit(NewBinaryOperatorNode<ArangoDBExpression> expression);

    protected abstract void visit(NewUnaryPrefixOperatorNode<ArangoDBExpression> expression);

    public void visit(Node<ArangoDBExpression> expressionNode) {
        if (expressionNode instanceof ArangoDBSelect) {
            visit((ArangoDBSelect<ArangoDBExpression>) expressionNode);
        } else if (expressionNode instanceof ColumnReferenceNode) {
            visit((ColumnReferenceNode<?, ?>) expressionNode);
        } else if (expressionNode instanceof ArangoDBConstant) {
            visit((ArangoDBConstant) expressionNode);
        } else if (expressionNode instanceof NewBinaryOperatorNode<?>) {
            visit((NewBinaryOperatorNode<ArangoDBExpression>) expressionNode);
        } else if (expressionNode instanceof NewUnaryPrefixOperatorNode<?>) {
            visit((NewUnaryPrefixOperatorNode<ArangoDBExpression>) expressionNode);
        } else {
            System.out.println(expressionNode.getClass());
            System.exit(0);
            throw new AssertionError(expressionNode);
        }
    }

    public static ArangoDBSelectQuery asSelectQuery(Node<ArangoDBExpression> expressionNode) {
        ArangoDBToQueryVisitor visitor = new ArangoDBToQueryVisitor();
        visitor.visit(expressionNode);
        return visitor.getQuery();
    }
}
