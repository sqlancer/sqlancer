package sqlancer.mongodb;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;

public abstract class MongoDBQueryAdapter extends Query<MongoDBConnection> {
    @Override
    public String getQueryString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUnterminatedQueryString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return new ExpectedErrors();
    }
}
