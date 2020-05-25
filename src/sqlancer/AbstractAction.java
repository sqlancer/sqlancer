package sqlancer;

import java.sql.SQLException;

public interface AbstractAction<G> {
	public Query getQuery(G globalState) throws SQLException;
}
