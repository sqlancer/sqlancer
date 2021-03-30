package sqlancer.arangodb;

import sqlancer.common.query.Query;

public abstract class ArangoDBQueryAdapter extends Query<ArangoDBConnection> {
    @Override
    public String getQueryString() {
        // Should not be called as it is used only in SQL dependent classes
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUnterminatedQueryString() {
        throw new UnsupportedOperationException();
    }
}
