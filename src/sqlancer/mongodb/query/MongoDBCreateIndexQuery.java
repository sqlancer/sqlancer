package sqlancer.mongodb.query;

import java.util.ArrayList;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Indexes;

import sqlancer.GlobalState;
import sqlancer.Main;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mongodb.MongoDBConnection;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.MongoDBSchema.MongoDBTable;

public class MongoDBCreateIndexQuery extends MongoDBQueryAdapter {

    private final MongoDBTable table;
    private final List<Bson> indeces;
    private final List<String> logIndeces;

    public MongoDBCreateIndexQuery(MongoDBTable table) {
        this.table = table;
        this.indeces = new ArrayList<>();
        this.logIndeces = new ArrayList<>();
    }

    public void addIndex(String column, boolean ascending) {
        if (ascending) {
            indeces.add(Indexes.ascending(column));
            logIndeces.add(column + ": 1");
        } else {
            indeces.add(Indexes.descending(column));
            logIndeces.add(column + ": -1");
        }
    }

    @Override
    public String getLogString() {
        StringBuilder sb = new StringBuilder();
        sb.append("db.").append(table.getName()).append(".createIndex({");
        String helper = "";
        for (String index : logIndeces) {
            sb.append(helper);
            helper = ",";
            sb.append(index);
        }
        sb.append("})\n");
        return sb.toString();
    }

    @Override
    public boolean couldAffectSchema() {
        return false;
    }

    @Override
    public <G extends GlobalState<?, ?, MongoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        Main.nrSuccessfulActions.addAndGet(1);
        Bson index;
        if (indeces.size() > 1) {
            index = Indexes.compoundIndex(indeces);
        } else {
            index = indeces.get(0);
        }
        globalState.getConnection().getDatabase().getCollection(table.getName()).createIndex(index);
        return true;
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return new ExpectedErrors();
    }

}
