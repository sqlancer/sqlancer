package sqlancer.arangodb.visitor;

import sqlancer.arangodb.ast.ArangoDBConstant;
import sqlancer.arangodb.ast.ArangoDBExpression;
import sqlancer.arangodb.ast.ArangoDBSelect;
import sqlancer.arangodb.query.ArangoDBSelectQuery;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;

public abstract class ArangoDBVisitor<E> {

    protected abstract void visit(ArangoDBSelect<E> expression);

    protected abstract void visit(ColumnReferenceNode<E, ?> expression);

    protected abstract void visit(ArangoDBConstant expression);

    protected abstract void visit(NewBinaryOperatorNode<E> expression);

    protected abstract void visit(NewUnaryPrefixOperatorNode<E> expression);

    protected abstract void visit(NewFunctionNode<E, ?> expression);

    @SuppressWarnings("unchecked")
    public void visit(Node<E> expressionNode) {
        if (expressionNode instanceof ArangoDBSelect) {
            visit((ArangoDBSelect<E>) expressionNode);
        } else if (expressionNode instanceof ColumnReferenceNode<?, ?>) {
            visit((ColumnReferenceNode<E, ?>) expressionNode);
        } else if (expressionNode instanceof ArangoDBConstant) {
            visit((ArangoDBConstant) expressionNode);
        } else if (expressionNode instanceof NewBinaryOperatorNode<?>) {
            visit((NewBinaryOperatorNode<E>) expressionNode);
        } else if (expressionNode instanceof NewUnaryPrefixOperatorNode<?>) {
            visit((NewUnaryPrefixOperatorNode<E>) expressionNode);
        } else if (expressionNode instanceof NewFunctionNode<?, ?>) {
            visit((NewFunctionNode<E, ?>) expressionNode);
        } else {
            throw new AssertionError(expressionNode);
        }
    }

    public static ArangoDBSelectQuery asSelectQuery(Node<ArangoDBExpression> expressionNode) {
        ArangoDBToQueryVisitor visitor = new ArangoDBToQueryVisitor();
        visitor.visit(expressionNode);
        return visitor.getQuery();
    }
}
