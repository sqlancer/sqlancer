package sqlancer.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.GlobalState;
import sqlancer.Randomly;

public class PostgresGlobalState extends GlobalState {

	private List<String> operators;
	private List<String> collates;
	private List<String> opClasses;
	private PostgresSchema schema;
	private PostgresOptions postgresOptions;

	@Override
	public void setConnection(Connection con) {
		super.setConnection(con);
		try {
			this.opClasses = getOpclasses(getConnection());
			this.operators = getOperators(getConnection());
			this.collates = getCollnames(getConnection());
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
	}

	public void setSchema(PostgresSchema schema) {
		this.schema = schema;
	}

	public PostgresSchema getSchema() {
		return schema;
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

	public void setPostgresOptions(PostgresOptions postgresOptions) {
		this.postgresOptions = postgresOptions;
	}

	public PostgresOptions getPostgresOptions() {
		return postgresOptions;
	}
	
}
