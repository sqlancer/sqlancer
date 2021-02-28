package sqlancer.arangodb;

import sqlancer.common.query.Query;

public abstract class ArangoDBQueryAdapter extends Query<ArangoDBConnection> {
    @Override
    public String getQueryString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUnterminatedQueryString() {
        throw new UnsupportedOperationException();
    }
}
