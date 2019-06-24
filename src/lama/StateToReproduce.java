package lama;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.schema.SQLite3Schema.Column;

public abstract class StateToReproduce {

	public enum ErrorKind {
		EXCEPTION, ROW_NOT_FOUND
	}

	ErrorKind errorKind;

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
		assert this.errorKind == ErrorKind.EXCEPTION;
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

	public ErrorKind getErrorKind() {
		return errorKind;
	}

	public void setErrorKind(ErrorKind errorKind) {
		this.errorKind = errorKind;
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
	
	public static class SQLite3StateToReproduce extends StateToReproduce {
		public SQLite3StateToReproduce(String databaseName) {
			super(databaseName);
		}

		public Map<Column, SQLite3Constant> getRandomRowValues() {
			return randomRowValues;
		}
		
		public Map<Column, SQLite3Constant> randomRowValues;
		
		public SQLite3Expression whereClause;
		
		public SQLite3Expression getWhereClause() {
			return whereClause;
		}

	}

}
