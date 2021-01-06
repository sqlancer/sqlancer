package sqlancer.mongodb.visitor;

import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

import java.util.ArrayList;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;

import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBToQueryVisitor extends MongoDBVisitor {

    private List<Bson> lookup;
    private Bson projection;

    @Override
    public void visit(MongoDBConstant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MongoDBSelect<MongoDBExpression> select) {
        setLookup(select);
        // if(select.hasWhere()) {
        // // DO STUFF
        // }
        setProjection(select);
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
        // TODO Patrick: Add Match if where...
        result.add(projection);
        return result;
    }
}
