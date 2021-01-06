package sqlancer.mongodb.gen;

import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.mongodb.ast.MongoDBDummyPredicate;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBExpressionGenerator
        extends UntypedExpressionGenerator<Node<MongoDBExpression>, MongoDBColumnTestReference> {
    /*
     * private final MongoDBGlobalState globalState;
     *
     * public MongoDBExpressionGenerator(MongoDBGlobalState globalState) { this.globalState = globalState; }
     */
    @Override
    public Node<MongoDBExpression> generateConstant() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Node<MongoDBExpression> generateExpression(int depth) {
        return new MongoDBDummyPredicate<>();
    }

    @Override
    protected Node<MongoDBExpression> generateColumn() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node<MongoDBExpression> negatePredicate(Node<MongoDBExpression> predicate) {
        return new MongoDBDummyPredicate<>();
    }

    @Override
    public Node<MongoDBExpression> isNull(Node<MongoDBExpression> expr) {
        return new MongoDBDummyPredicate<>();
    }
}
