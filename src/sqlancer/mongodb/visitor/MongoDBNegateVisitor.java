package sqlancer.mongodb.visitor;

import static sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBBinaryLogicalOperator.AND;
import static sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBBinaryLogicalOperator.NOR;
import static sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBBinaryLogicalOperator.OR;
import static sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBUnaryLogicalOperator.NOT;

import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.ast.MongoDBBinaryComparisonNode;
import sqlancer.mongodb.ast.MongoDBBinaryLogicalNode;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBRegexNode;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.ast.MongoDBUnaryLogicalOperatorNode;
import sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator;

public class MongoDBNegateVisitor extends MongoDBVisitor {

    private boolean negate;
    Node<MongoDBExpression> negatedExpression;

    public MongoDBNegateVisitor(boolean negate) {
        this.negate = negate;
    }

    @Override
    public void visit(Node<MongoDBExpression> expr) {
        if (expr instanceof MongoDBConstant) {
            visit((MongoDBConstant) expr);
        } else if (expr instanceof MongoDBSelect) {
            visit((MongoDBSelect<MongoDBExpression>) expr);
        } else if (expr instanceof MongoDBBinaryComparisonNode) {
            visit((MongoDBBinaryComparisonNode) expr);
        } else if (expr instanceof MongoDBUnaryLogicalOperatorNode) {
            visit((MongoDBUnaryLogicalOperatorNode) expr);
        } else if (expr instanceof MongoDBRegexNode) {
            visit((MongoDBRegexNode) expr);
        } else if (expr instanceof MongoDBBinaryLogicalNode) {
            visit((MongoDBBinaryLogicalNode) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    public void visit(MongoDBBinaryComparisonNode expr) {

        if (negate) {
            negatedExpression = new MongoDBUnaryLogicalOperatorNode(expr, NOT);
            switch (expr.operator()) {
            case EQUALS:
                negatedExpression = new MongoDBBinaryComparisonNode(expr.getLeft(), expr.getRight(),
                        MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator.NOT_EQUALS);
                break;
            case NOT_EQUALS:
                negatedExpression = new MongoDBBinaryComparisonNode(expr.getLeft(), expr.getRight(),
                        MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator.EQUALS);
                break;
            case LESS:
                negatedExpression = new MongoDBBinaryComparisonNode(expr.getLeft(), expr.getRight(),
                        MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator.GREATER_EQUAL);
                break;
            case LESS_EQUAL:
                negatedExpression = new MongoDBBinaryComparisonNode(expr.getLeft(), expr.getRight(),
                        MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator.GREATER);
                break;
            case GREATER:
                negatedExpression = new MongoDBBinaryComparisonNode(expr.getLeft(), expr.getRight(),
                        MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator.LESS_EQUAL);
                break;
            case GREATER_EQUAL:
                negatedExpression = new MongoDBBinaryComparisonNode(expr.getLeft(), expr.getRight(),
                        MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator.LESS);
                break;
            default:
                throw new UnsupportedOperationException();
            }
        } else {
            negatedExpression = expr;
        }
    }

    public void visit(MongoDBRegexNode expr) {
        if (negate) {
            negatedExpression = new MongoDBUnaryLogicalOperatorNode(expr, NOT);
        } else {
            negatedExpression = expr;
        }
    }

    public void visit(MongoDBUnaryLogicalOperatorNode expr) {
        if (!(expr.operator().equals(NOT))) {
            throw new UnsupportedOperationException();
        }
        negate = !negate;
        visit(expr.getExpr());
    }

    public void visit(MongoDBBinaryLogicalNode expr) {
        boolean saveNegate = negate;
        Node<MongoDBExpression> left;
        Node<MongoDBExpression> right;
        switch (expr.operator()) {
        case OR:
            negate = false;
            visit(expr.getLeft());
            left = negatedExpression;
            negate = false;
            visit(expr.getRight());
            right = negatedExpression;
            if (saveNegate) {
                negatedExpression = new MongoDBBinaryLogicalNode(left, right, NOR);
            } else {
                negatedExpression = new MongoDBBinaryLogicalNode(left, right, OR);
            }
            break;
        case AND:
            negate = saveNegate;
            visit(expr.getLeft());
            left = negatedExpression;
            negate = saveNegate;
            visit(expr.getRight());
            right = negatedExpression;
            if (saveNegate) {
                negatedExpression = new MongoDBBinaryLogicalNode(left, right, OR);
            } else {
                negatedExpression = new MongoDBBinaryLogicalNode(left, right, AND);
            }
            break;
        case NOR:
            negate = false;
            visit(expr.getLeft());
            left = negatedExpression;
            negate = false;
            visit(expr.getRight());
            right = negatedExpression;
            if (saveNegate) {
                negatedExpression = new MongoDBBinaryLogicalNode(left, right, OR);
            } else {
                negatedExpression = new MongoDBBinaryLogicalNode(left, right, NOR);
            }
            break;
        default:
            throw new UnsupportedOperationException(expr.getOperatorRepresentation());
        }
    }

    @Override
    public void visit(MongoDBConstant c) {
        negatedExpression = c;
    }

    @Override
    public void visit(MongoDBSelect<MongoDBExpression> s) {
        throw new UnsupportedOperationException();
    }

    public Node<MongoDBExpression> getNegatedExpression() {
        return negatedExpression;
    }
}
