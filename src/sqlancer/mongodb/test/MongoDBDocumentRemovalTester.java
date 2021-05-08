package sqlancer.mongodb.test;

import static sqlancer.mongodb.MongoDBComparatorHelper.getResultSetAsDocumentList;

import java.util.List;

import org.bson.Document;

import sqlancer.Randomly;
import sqlancer.mongodb.MongoDBProvider;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.gen.MongoDBInsertGenerator;
import sqlancer.mongodb.query.MongoDBRemoveQuery;
import sqlancer.mongodb.query.MongoDBSelectQuery;

public class MongoDBDocumentRemovalTester extends MongoDBDocumentRemovalBase {
    public MongoDBDocumentRemovalTester(MongoDBProvider.MongoDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();

        select.setWithCountClause(false);

        select.setFilterClause(predicate);
        MongoDBSelectQuery selectQuery = new MongoDBSelectQuery(select);
        List<Document> firstResultSet = getResultSetAsDocumentList(selectQuery, state);
        if (firstResultSet == null || firstResultSet.isEmpty()) {
            return;
        }

        Document documentToRemove = Randomly.fromList(firstResultSet);
        MongoDBRemoveQuery removeQuery = new MongoDBRemoveQuery(mainTable, documentToRemove.get("_id").toString());
        state.executeStatement(removeQuery);

        selectQuery = new MongoDBSelectQuery(select);
        List<Document> secondResultSet = getResultSetAsDocumentList(selectQuery, state);

        MongoDBQueryAdapter insertQuery = MongoDBInsertGenerator.getQuery(state);
        state.executeStatement(insertQuery);

        if (secondResultSet.size() + 1 != firstResultSet.size()) {
            String assertMessage = "The Result Sizes mismatches!";
            throw new AssertionError(assertMessage);
        }
    }
}
