package lama.mysql;

import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.Main.StateToReproduce;
import lama.Randomly;

public class MySQLQueryGenerator {

	private QueryManager manager;
	private Randomly r;

	public MySQLQueryGenerator(QueryManager manager, Randomly r) {
		this.manager = manager;
		this.r = r;
	}

	public void generateAndCheckQuery(StateToReproduce state, StateLogger logger) {
		// TODO Auto-generated method stub
		
	}

}
