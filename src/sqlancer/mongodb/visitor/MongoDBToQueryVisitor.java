package sqlancer.mongodb.visitor;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

import java.util.ArrayList;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;

import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.ast.MongoDBBinaryComparisonNode;
import sqlancer.mongodb.ast.MongoDBBinaryLogicalNode;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.ast.MongoDBUnaryLogicalOperatorNode;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBToQueryVisitor extends MongoDBVisitor {

    private List<Bson> lookup;
    private Bson filter;
    private Bson projection;
    private boolean hasFilter;

    public Bson visitBson(Node<MongoDBExpression> expr) {
        if (expr instanceof MongoDBUnaryLogicalOperatorNode) {
            return visit((MongoDBUnaryLogicalOperatorNode) expr);
        } else if (expr instanceof MongoDBBinaryLogicalNode) {
            return visit((MongoDBBinaryLogicalNode) expr);
        } else if (expr instanceof MongoDBBinaryComparisonNode) {
            return visit((MongoDBBinaryComparisonNode) expr);
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
        projection = project(fields(include(stringProjects)));
    }

    public List<Bson> getPipeline() {
        List<Bson> result = new ArrayList<>(lookup);
        if (hasFilter) {
            result.add(filter);
        }
        result.add(projection);
        return result;
    }
}
