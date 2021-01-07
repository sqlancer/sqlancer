package sqlancer.mongodb.test;

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

        select.setFilterClause(null);
        MongoDBSelectQuery q = new MongoDBSelectQuery(select);
        q.executeAndGet(state);

        List<Document> firstResultSet = q.getResultSet();
        select.setFilterClause(predicate);
        q = new MongoDBSelectQuery(select);
        q.executeAndGet(state);
        List<Document> secondResultSet = q.getResultSet();

        select.setFilterClause(negatedPredicate);
        q = new MongoDBSelectQuery(select);
        q.executeAndGet(state);
        List<Document> thirdResultSet = q.getResultSet();

        secondResultSet.addAll(thirdResultSet);
        MongoDBComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, q);

    }
}
