package sqlancer.mongodb.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import sqlancer.GlobalState;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mongodb.MongoDBConnection;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.visitor.MongoDBVisitor;

public class MongoDBSelectQuery extends MongoDBQueryAdapter {

    private final MongoDBSelect<MongoDBExpression> select;

    private List<Document> resultSet;

    public MongoDBSelectQuery(MongoDBSelect<MongoDBExpression> select) {
        this.select = select;
    }

    @Override
    public boolean couldAffectSchema() {
        return false;
    }

    @Override
    public <G extends GlobalState<?, ?, MongoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <G extends GlobalState<?, ?, MongoDBConnection>> SQLancerResultSet executeAndGet(G globalState,
            String... fills) throws Exception {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(this.getLogString());
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        List<Bson> pipeline = MongoDBVisitor.asQuery(select);

        MongoCollection<Document> collection = globalState.getConnection().getDatabase()
                .getCollection(select.getMainTableName());
        MongoCursor<Document> cursor = collection.aggregate(pipeline).cursor();
        resultSet = new ArrayList<>();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            resultSet.add(document);
        }
        return null;
    }

    @Override
    public String getLogString() {
        return MongoDBVisitor.asStringLog(select);
    }

    public List<Document> getResultSet() {
        return resultSet;
    }

}
