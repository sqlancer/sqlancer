package sqlancer.mongodb.visitor;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;

import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.ast.MongoDBBinaryComparisonNode;
import sqlancer.mongodb.ast.MongoDBBinaryLogicalNode;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBStringConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBRegexNode;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.ast.MongoDBUnaryLogicalOperatorNode;
import sqlancer.mongodb.gen.MongoDBComputedExpressionGenerator.ComputedFunction;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBToQueryVisitor extends MongoDBVisitor {

    private List<Bson> lookup;
    private Bson filter;
    private Bson projection;
    private Bson count;
    private boolean hasFilter;
    private boolean hasCountClause;

    public Bson visitBson(Node<MongoDBExpression> expr) {
        if (expr instanceof MongoDBUnaryLogicalOperatorNode) {
            return visit((MongoDBUnaryLogicalOperatorNode) expr);
        } else if (expr instanceof MongoDBBinaryLogicalNode) {
            return visit((MongoDBBinaryLogicalNode) expr);
        } else if (expr instanceof MongoDBBinaryComparisonNode) {
            return visit((MongoDBBinaryComparisonNode) expr);
        } else if (expr instanceof MongoDBRegexNode) {
            return visit((MongoDBRegexNode) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    public Document visitComputed(Node<MongoDBExpression> expr) {
        if (expr instanceof NewFunctionNode<?, ?>) {
            return visitComputed((NewFunctionNode<?, ?>) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    public Document visitComputed(NewFunctionNode<?, ?> expr) {
        List<Serializable> visitedArgs = new ArrayList<>();
        for (int i = 0; i < expr.getArgs().size(); i++) {
            if (expr.getArgs().get(i) instanceof MongoDBConstant) {
                visitedArgs.add(((MongoDBConstant) expr.getArgs().get(i)).getSerializedValue());
                continue;
            }
            if (expr.getArgs().get(i) instanceof MongoDBColumnTestReference) {
                visitedArgs.add("$" + ((MongoDBColumnTestReference) expr.getArgs().get(i)).getQueryString());
                continue;
            }
            if (expr.getArgs().get(i) instanceof NewFunctionNode<?, ?>) {
                visitedArgs.add(visitComputed((NewFunctionNode<?, ?>) expr.getArgs().get(i)));
            } else {
                throw new AssertionError();
            }
        }
        if (expr.getFunc() instanceof ComputedFunction) {
            return new Document(((ComputedFunction) expr.getFunc()).getOperator(), visitedArgs);
        } else {
            throw new AssertionError(expr.getClass());
        }

    }

    public Bson visit(MongoDBUnaryLogicalOperatorNode expr) {
        Bson inner = visitBson(expr.getExpr());
        return expr.operator().applyOperator(inner);
    }

    public Bson visit(MongoDBBinaryLogicalNode expr) {
        Bson left = visitBson(expr.getLeft());
        Bson right = visitBson(expr.getRight());
        return expr.operator().applyOperator(left, right);
    }

    public Bson visit(MongoDBRegexNode expr) {
        Node<MongoDBExpression> left = expr.getLeft();
        Node<MongoDBExpression> right = expr.getRight();

        String columnName = ((MongoDBColumnTestReference) left).getQueryString();

        return expr.operator().applyOperator(columnName, (MongoDBStringConstant) right, expr.getOptions());
    }

    public Bson visit(MongoDBBinaryComparisonNode expr) {
        Node<MongoDBExpression> left = expr.getLeft();
        Node<MongoDBExpression> right = expr.getRight();
        assert left instanceof MongoDBColumnTestReference;
        assert right instanceof MongoDBConstant;

        String columnName = ((MongoDBColumnTestReference) left).getQueryString();
        return expr.operator().applyOperator(columnName, (MongoDBConstant) right);
    }

    @Override
    public void visit(MongoDBConstant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MongoDBSelect<MongoDBExpression> select) {
        hasFilter = select.hasFilter();
        setLookup(select);
        if (hasFilter) {
            setFilter(select);
        }
        setProjection(select);
        hasCountClause = select.getWithCountClause();
        if (hasCountClause) {
            setCount();
        }
    }

    private void setCount() {
        count = Aggregates.count("count");
    }

    private void setFilter(MongoDBSelect<MongoDBExpression> select) {
        filter = match(this.visitBson(select.getFilterClause()));
    }

    private void setLookup(MongoDBSelect<MongoDBExpression> select) {
        lookup = new ArrayList<>();
        for (MongoDBColumnTestReference reference : select.getLookupList()) {
            if (reference.inMainTable()) {
                continue;
            }
            lookup.add(Aggregates.lookup(reference.getTableName(), select.getJoinColumn().getPlainName(),
                    reference.getPlainName(), reference.getQueryString()));
        }
    }

    private void setProjection(MongoDBSelect<MongoDBExpression> select) {
        List<String> stringProjects = new ArrayList<>();
        for (MongoDBColumnTestReference ref : select.getProjectionList()) {
            stringProjects.add(ref.getQueryString());
        }
        List<Bson> projections = new ArrayList<>();
        projections.add(include(stringProjects));
        if (select.hasComputed()) {
            String name = "computed";
            int number = 0;
            for (Node<MongoDBExpression> expressionNode : select.getComputedClause()) {
                projections.add(Projections.computed(name + number, visitComputed(expressionNode)));
                number++;
            }
        }
        projection = project(fields(projections));
    }

    public List<Bson> getPipeline() {
        List<Bson> result = new ArrayList<>(lookup);
        if (hasFilter) {
            result.add(filter);
        }
        result.add(projection);
        if (hasCountClause) {
            result.add(count);
        }
        return result;
    }
}
