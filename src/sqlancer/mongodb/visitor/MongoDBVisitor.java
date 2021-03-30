package sqlancer.mongodb.visitor;

import java.util.List;

import org.bson.conversions.Bson;

import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBSelect;

public abstract class MongoDBVisitor {

    public abstract void visit(MongoDBConstant c);

    public abstract void visit(MongoDBSelect<MongoDBExpression> s);

    public void visit(Node<MongoDBExpression> expr) {
        if (expr instanceof MongoDBConstant) {
            visit((MongoDBConstant) expr);
        } else if (expr instanceof MongoDBSelect) {
            visit((MongoDBSelect<MongoDBExpression>) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    public static List<Bson> asQuery(Node<MongoDBExpression> expr) {
        MongoDBToQueryVisitor visitor = new MongoDBToQueryVisitor();
        visitor.visit(expr);
        return visitor.getPipeline();
    }

    public static String asStringLog(Node<MongoDBExpression> expr) {
        MongoDBToLogVisitor visitor = new MongoDBToLogVisitor();
        visitor.visit(expr);
        return visitor.getStringLog();
    }

    public static Node<MongoDBExpression> cleanNegations(Node<MongoDBExpression> expr) {
        MongoDBNegateVisitor visitor = new MongoDBNegateVisitor(false);
        visitor.visit(expr);
        return visitor.getNegatedExpression();
    }
}
