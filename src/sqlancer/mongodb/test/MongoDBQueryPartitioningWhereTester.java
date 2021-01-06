package sqlancer.mongodb.test;

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
        /*
         * List<Document> firstResultSet = q.getResultSet(); select.setWhereClause(predicate); q = new
         * MongoDBFindQueryAdapter(select, mainTable); q.executeAndGet(state); List<Document> secondResultSet =
         * q.resultSet;
         *
         * select.setWhereClause(negatedPredicate); q = new MongoDBFindQueryAdapter(select, mainTable);
         * q.executeAndGet(state); List<Document> thirdResultSet = q.resultSet;
         *
         * secondResultSet.addAll(thirdResultSet); MongoDBComparatorHelper.assumeResultSetsAreEqual(firstResultSet,
         * secondResultSet, q);
         *
         */
    }
}
