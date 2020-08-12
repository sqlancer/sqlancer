package sqlancer.common.query;

import java.sql.SQLException;

@FunctionalInterface
public interface QueryProvider<S> {
    Query getQuery(S globalState) throws SQLException;
}
