package sqlancer.mongodb.visitor;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.ast.MongoDBBinaryComparisonNode;
import sqlancer.mongodb.ast.MongoDBBinaryLogicalNode;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBRegexNode;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.ast.MongoDBUnaryLogicalOperatorNode;
import sqlancer.mongodb.gen.MongoDBComputedExpressionGenerator.ComputedFunction;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBToLogVisitor extends MongoDBVisitor {

    private String mainTableName;
    private List<String> lookups;
    private String filter;
    private String projects;
    private boolean hasFilter;
    private boolean withCount;

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

    public String visitComputed(Node<MongoDBExpression> expr) {
        if (expr instanceof NewFunctionNode<?, ?>) {
            return visitComputed((NewFunctionNode<?, ?>) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    public String visitComputed(NewFunctionNode<?, ?> expr) {
        List<String> arguments = new ArrayList<>();
        for (int i = 0; i < expr.getArgs().size(); i++) {
            if (expr.getArgs().get(i) instanceof MongoDBConstant) {
                arguments.add(((MongoDBConstant) expr.getArgs().get(i)).getLogValue());
                continue;
            }
            if (expr.getArgs().get(i) instanceof MongoDBColumnTestReference) {
                arguments.add("\"$" + ((MongoDBColumnTestReference) expr.getArgs().get(i)).getQueryString() + "\"");
                continue;
            }
            if (expr.getArgs().get(i) instanceof NewFunctionNode<?, ?>) {
                arguments.add(visitComputed((NewFunctionNode<?, ?>) expr.getArgs().get(i)));
            } else {
                throw new AssertionError();
            }
        }
        if (!(expr.getFunc() instanceof ComputedFunction)) {
            throw new AssertionError(expr.getClass());
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(((ComputedFunction) expr.getFunc()).getOperator());
        sb.append(": [");
        String helper = "";
        for (String arg : arguments) {
            sb.append(helper);
            helper = ", ";
            sb.append(arg);
        }
        sb.append("]}");
        return sb.toString();
    }

    public String visit(MongoDBUnaryLogicalOperatorNode expr) {
        String inner = visitLog(expr.getExpr());
        return "{ " + expr.operator().getTextRepresentation() + ": [" + inner + "]}";
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

        return "{\"" + ((MongoDBColumnTestReference) left).getQueryString() + "\": {"
                + expr.operator().getTextRepresentation() + ": \'"
                + ((MongoDBConstant.MongoDBStringConstant) right).getStringValue() + "\', $options: \'"
                + expr.getOptions() + "\'}}";
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
        withCount = select.getWithCountClause();
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
        sb.append("\n");
        if (select.hasComputed()) {
            String name = "computed";
            int number = 0;
            for (Node<MongoDBExpression> expressionNode : select.getComputedClause()) {
                sb.append(helper);
                helper = ",\n";
                sb.append("\"" + name + number + "\": " + visitComputed(expressionNode));
                number++;
            }
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
        sb.append("}");
        if (withCount) {
            sb.append(",\n");
            sb.append(" {$count: \"count\"}\n");
        }
        sb.append("])\n");
        return sb.toString();
    }
}
