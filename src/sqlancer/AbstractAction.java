package sqlancer;

import java.sql.SQLException;

public interface AbstractAction<G> {

	Query getQuery(G globalState) throws SQLException;

}
