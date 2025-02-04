package sqlancer;

import org.junit.jupiter.api.Test;
import sqlancer.common.log.SQLLoggableFactory;
import sqlancer.common.query.SQLQueryAdapter;

public class TestLoggableFactory {

    @Test
    public void testLogCreateTable() {
        String query = "CREATE TABLE t1 (c1 INT)";
        SQLLoggableFactory logger = new SQLLoggableFactory();
        SQLQueryAdapter queryAdapter = logger.getQueryForStateToReproduce(query);
        assert (queryAdapter.couldAffectSchema());
    }

}
