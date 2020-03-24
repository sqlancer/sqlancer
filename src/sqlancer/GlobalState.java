package sqlancer;

import java.sql.Connection;

import sqlancer.Main.StateLogger;

public class GlobalState {

	private Connection con;
	private Randomly r;
	private MainOptions options;
	private StateLogger logger;
	private StateToReproduce state;

	public void setConnection(Connection con) {
		this.con = con;
	}

	public Connection getConnection() {
		return con;
	}

	public void setRandomly(Randomly r) {
		this.r = r;
	}

	public Randomly getRandomly() {
		return r;
	}

	public MainOptions getOptions() {
		return options;
	}

	public void setMainOptions(MainOptions options) {
		this.options = options;
	}

	public void setStateLogger(StateLogger logger) {
		this.logger = logger;
	}

	public StateLogger getLogger() {
		return logger;
	}

	public void setState(StateToReproduce state) {
		this.state = state;
	}

	public StateToReproduce getState() {
		return state;
	}

}