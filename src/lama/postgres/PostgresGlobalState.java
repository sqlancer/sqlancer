package lama.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import lama.Randomly;

public class PostgresGlobalState {
	
	private final List<String> operators;
	private final List<String> collates;
	private final List<String> opClasses;
	private final Connection con;
	
	public PostgresGlobalState(Connection con) {
		this.con = con;
		try {
			this.opClasses = getOpclasses(con);
			this.operators = getOperators(con);
			this.collates = getCollnames(con);
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
	}
	
	public Connection getCon() {
		return con;
	}
	
	private List<String> getCollnames(Connection con) throws SQLException {
		List<String> opClasses = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s
					.executeQuery("SELECT collname FROM pg_collation WHERE collname LIKE '%utf8' or collname = 'C';")) {
				while (rs.next()) {
					opClasses.add(rs.getString(1));
				}
			}
		}
		return opClasses;
	}

	private List<String> getOpclasses(Connection con) throws SQLException {
		List<String> opClasses = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("select opcname FROM pg_opclass;")) {
				while (rs.next()) {
					opClasses.add(rs.getString(1));
				}
			}
		}
		return opClasses;
	}

	private List<String> getOperators(Connection con) throws SQLException {
		List<String> opClasses = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("SELECT oprname FROM pg_operator;")) {
				while (rs.next()) {
					opClasses.add(rs.getString(1));
				}
			}
		}
		return opClasses;
	}



	public List<String> getOperators() {
		return operators;
	}
	
	public String getRandomOperator() {
		return Randomly.fromList(operators);
	}
	
	public List<String> getCollates() {
		return collates;
	}
	
	public String getRandomCollate() {
		return Randomly.fromList(collates);
	}
	
	public List<String> getOpClasses() {
		return opClasses;
	}
	
	public String getRandomOpclass() {
		return Randomly.fromList(opClasses);
	}
 
}
