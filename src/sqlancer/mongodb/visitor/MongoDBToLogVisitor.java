package sqlancer.mongodb.visitor;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.ast.MongoDBBinaryComparisonNode;
import sqlancer.mongodb.ast.MongoDBBinaryLogicalNode;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBRegexNode;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.ast.MongoDBUnaryLogicalOperatorNode;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBToLogVisitor extends MongoDBVisitor {

    private String mainTableName;
    private List<String> lookups;
    private String filter;
    private String projects;
    private boolean hasFilter;

    public String visitLog(Node<MongoDBExpression> expr) {
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

    public String visit(MongoDBUnaryLogicalOperatorNode expr) {
        String inner = visitLog(expr.getExpr());
        return expr.operator().getTextRepresentation() + inner + "]}";
    }

    public String visit(MongoDBBinaryLogicalNode expr) {
        String left = visitLog(expr.getLeft());
        String right = visitLog(expr.getRight());

        return "{" + expr.operator().getTextRepresentation() + ":[" + left + "," + right + "]}";
    }

    public String visit(MongoDBBinaryComparisonNode expr) {
        Node<MongoDBExpression> left = expr.getLeft();
        Node<MongoDBExpression> right = expr.getRight();
        assert left instanceof MongoDBColumnTestReference;
        assert right instanceof MongoDBConstant;

        return "{\"" + ((MongoDBColumnTestReference) left).getQueryString() + "\": {"
                + expr.operator().getTextRepresentation() + ": " + ((MongoDBConstant) right).getLogValue() + "}}";
    }

    public String visit(MongoDBRegexNode expr) {
        Node<MongoDBExpression> left = expr.getLeft();
        Node<MongoDBExpression> right = expr.getRight();
        assert left instanceof MongoDBColumnTestReference;
        assert right instanceof MongoDBConstant.MongoDBStringConstant;

        return "{\"" + ((MongoDBColumnTestReference) left).getQueryString() + "\": {"
                + expr.operator().getTextRepresentation() + ": \'"
                + ((MongoDBConstant.MongoDBStringConstant) right).getStringValue() + "\', $options: \'\'}}";
    }

    @Override
    public void visit(MongoDBConstant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MongoDBSelect<MongoDBExpression> select) {
        hasFilter = select.hasFilter();
        mainTableName = select.getMainTableName();
        setLookups(select);
        if (hasFilter) {
            setFilter(select);
        }
        setProjects(select);
    }

    private void setFilter(MongoDBSelect<MongoDBExpression> select) {
        filter = visitLog(select.getFilterClause());
    }

    private void setLookups(MongoDBSelect<MongoDBExpression> select) {
        lookups = new ArrayList<>();
        for (MongoDBColumnTestReference testReference : select.getLookupList()) {
            if (testReference.inMainTable()) {
                continue;
            }
            String newLookup = "{ $lookup: { from: \"" + testReference.getTableName() + "\", localField: \""
                    + select.getJoinColumn().getPlainName() + "\", foreignField: \"" + testReference.getPlainName()
                    + "\", as: \"" + testReference.getQueryString() + "\"}},\n";
            lookups.add(newLookup);
        }
    }

    private void setProjects(MongoDBSelect<MongoDBExpression> select) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        String helper = "";
        for (MongoDBColumnTestReference reference : select.getProjectionList()) {
            sb.append(helper);
            helper = ",";
            sb.append("\"").append(reference.getQueryString()).append("\"").append(": 1");
        }
        sb.append("}");
        projects = sb.toString();
    }

    public String getStringLog() {
        StringBuilder sb = new StringBuilder();
        sb.append("db.").append(mainTableName).append(".aggregate([\n");
        for (String lookup : lookups) {
            sb.append(lookup);
        }
        if (hasFilter) {
            sb.append("{ $match: ");
            sb.append(filter);
            sb.append("},\n");
        }
        sb.append("{ $project : ");
        sb.append(projects);
        sb.append("}])\n");
        return sb.toString();
    }
}
