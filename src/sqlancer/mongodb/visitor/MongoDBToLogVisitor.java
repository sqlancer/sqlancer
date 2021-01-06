package sqlancer.mongodb.visitor;

import java.util.ArrayList;
import java.util.List;

import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBToLogVisitor extends MongoDBVisitor {

    private String mainTableName;
    private List<String> lookups;
    private String projects;

    @Override
    public void visit(MongoDBConstant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MongoDBSelect<MongoDBExpression> select) {
        mainTableName = select.getMainTableName();
        setLookups(select);
        setProjects(select);
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
        // sb.append(",\n");
        // if(select.hasWhere()) {
        // sb.append("{ $match: ");
        // sb.append(mongoDBToQueryVisitor.getFilterLog());
        // sb.append("},\n");
        // }
        sb.append("{ $project : ");
        sb.append(projects);
        sb.append("}])\n");
        return sb.toString();
    }
}
