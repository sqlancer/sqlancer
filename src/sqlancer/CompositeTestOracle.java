package sqlancer;

import java.sql.SQLException;
import java.util.List;

public class CompositeTestOracle implements TestOracle {

	private final TestOracle[] oracles;
	private int i;

	public CompositeTestOracle(List<TestOracle> oracles) {
		this.oracles = oracles.toArray(new TestOracle[oracles.size()]);
	}

	@Override
	public void check() throws SQLException {
		try {
			oracles[i].check();
		} finally {
			i = (i + 1) % oracles.length;
		}
	}
}
