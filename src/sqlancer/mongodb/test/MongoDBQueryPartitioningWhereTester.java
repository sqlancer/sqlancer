package sqlancer.mongodb.test;

import static sqlancer.mongodb.MongoDBComparatorHelper.getResultSetAsDocumentList;

import java.util.List;

import org.bson.Document;

import sqlancer.mongodb.MongoDBComparatorHelper;
import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;
import sqlancer.mongodb.query.MongoDBSelectQuery;

public class MongoDBQueryPartitioningWhereTester extends MongoDBQueryPartitioningBase {
    public MongoDBQueryPartitioningWhereTester(MongoDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();

        select.setWithCountClause(false);

        select.setFilterClause(null);
        MongoDBSelectQuery q = new MongoDBSelectQuery(select);
        List<Document> firstResultSet = getResultSetAsDocumentList(q, state);

        select.setFilterClause(predicate);
        q = new MongoDBSelectQuery(select);
        List<Document> secondResultSet = getResultSetAsDocumentList(q, state);

        select.setFilterClause(negatedPredicate);
        q = new MongoDBSelectQuery(select);
        List<Document> thirdResultSet = getResultSetAsDocumentList(q, state);

        if (state.getDmbsSpecificOptions().testWithCount) {
            select.setWithCountClause(true);
            select.setFilterClause(predicate);
            q = new MongoDBSelectQuery(select);
            List<Document> forthResultSet = getResultSetAsDocumentList(q, state);
            MongoDBComparatorHelper.assumeCountIsEqual(secondResultSet, forthResultSet, q);
        }

        secondResultSet.addAll(thirdResultSet);
        MongoDBComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, q);

    }
}
