package sqlancer.arangodb.test;

import static sqlancer.arangodb.ArangoDBComparatorHelper.assumeResultSetsAreEqual;
import static sqlancer.arangodb.ArangoDBComparatorHelper.getResultSetAsDocumentList;

import java.util.List;

import com.arangodb.entity.BaseDocument;

import sqlancer.arangodb.ArangoDBProvider;
import sqlancer.arangodb.query.ArangoDBSelectQuery;
import sqlancer.arangodb.visitor.ArangoDBVisitor;

public class ArangoDBQueryPartitioningWhereTester extends ArangoDBQueryPartitioningBase {
    public ArangoDBQueryPartitioningWhereTester(ArangoDBProvider.ArangoDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        select.setFilterClause(null);

        ArangoDBSelectQuery query = ArangoDBVisitor.asSelectQuery(select);
        List<BaseDocument> firstResultSet = getResultSetAsDocumentList(query, state);

        select.setFilterClause(predicate);
        query = ArangoDBVisitor.asSelectQuery(select);
        List<BaseDocument> secondResultSet = getResultSetAsDocumentList(query, state);

        select.setFilterClause(negatedPredicate);
        query = ArangoDBVisitor.asSelectQuery(select);
        List<BaseDocument> thirdResultSet = getResultSetAsDocumentList(query, state);

        thirdResultSet.addAll(secondResultSet);
        assumeResultSetsAreEqual(firstResultSet, thirdResultSet, query);

        if (state.getDmbsSpecificOptions().withOptimizerRuleTests) {
            select.setFilterClause(predicate);
            query = ArangoDBVisitor.asSelectQuery(select);
            query.excludeRandomOptRules();
            List<BaseDocument> forthResultSet = getResultSetAsDocumentList(query, state);
            assumeResultSetsAreEqual(secondResultSet, forthResultSet, query);
        }
    }
}
