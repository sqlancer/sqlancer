package lama;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.ast.PostgresConstant;
import lama.postgres.ast.PostgresExpression;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.schema.SQLite3Schema.SQLite3Column;
import lama.tdengine.TDEngineSchema.TDEngineColumn;
import lama.tdengine.expr.TDEngineConstant;
import lama.tdengine.expr.TDEngineExpression;
import lama.tdengine.expr.TDEngineSelectStatement;

public abstract class StateToReproduce {

	public final List<Query> statements = new ArrayList<>();
	public String queryString;

	private String databaseName;

	public String databaseVersion;

	public String values;

	String exception;

	public String queryTargetedTablesString;

	public String queryTargetedColumnsString;

	public StateToReproduce(String databaseName) {
		this.databaseName = databaseName;

	}

	public String getException() {
		return exception;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getDatabaseVersion() {
		return databaseVersion;
	}

	public List<Query> getStatements() {
		return statements;
	}

	public String getQueryString() {
		return queryString;
	}

	public static class MySQLStateToReproduce extends StateToReproduce {

		public MySQLStateToReproduce(String databaseName) {
			super(databaseName);
		}


		public Map<MySQLColumn, MySQLConstant> getRandomRowValues() {
			return randomRowValues;
		}
		
		public Map<MySQLColumn, MySQLConstant> randomRowValues;
		
		public MySQLExpression whereClause;

		public String queryThatSelectsRow;
		
		public MySQLExpression getWhereClause() {
			return whereClause;
		}
		
		
	}
	
	public static class MariaDBStateToReproduce extends StateToReproduce {

		public MariaDBStateToReproduce(String databaseName) {
			super(databaseName);
		}

		
	}
	
	public static class CockroachDBStateToReproduce extends StateToReproduce {

		public CockroachDBStateToReproduce(String databaseName) {
			super(databaseName);
		}

		
	}
	
	
	public static class TDEngineStateToReproduce extends StateToReproduce {
		public TDEngineStateToReproduce(String databaseName) {
			super(databaseName);
		}

		public Map<TDEngineColumn, TDEngineConstant> getRandomRowValues() {
			return randomRowValues;
		}
		
		public Map<TDEngineColumn, TDEngineConstant> randomRowValues;
		
		public TDEngineSelectStatement whereClause;
		
		public TDEngineExpression getWhereClause() {
			return whereClause;
		}

	}
	
	public static class SQLite3StateToReproduce extends StateToReproduce {
		public SQLite3StateToReproduce(String databaseName) {
			super(databaseName);
		}

		public Map<SQLite3Column, SQLite3Constant> getRandomRowValues() {
			return randomRowValues;
		}
		
		public Map<SQLite3Column, SQLite3Constant> randomRowValues;
		
		public SQLite3Expression whereClause;
		
		public SQLite3Expression getWhereClause() {
			return whereClause;
		}

	}
	
	public static class PostgresStateToReproduce extends StateToReproduce {
		public PostgresStateToReproduce(String databaseName) {
			super(databaseName);
		}

		public Map<PostgresColumn, PostgresConstant> getRandomRowValues() {
			return randomRowValues;
		}
		
		public Map<PostgresColumn, PostgresConstant> randomRowValues;
		
		public PostgresExpression whereClause;

		public String queryThatSelectsRow;
		
		public PostgresExpression getWhereClause() {
			return whereClause;
		}

	}

}
