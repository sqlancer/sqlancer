package sqlancer;

import java.sql.SQLException;

public interface TestOracle {

	public void check() throws SQLException;

	public default boolean onlyWorksForNonEmptyTables() {
		return false;
	}

}
