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
		oracles[i].check();
		i = (i + 1) % oracles.length;
	}
}
