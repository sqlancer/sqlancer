package lama;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.schema.SQLite3Schema.Column;

public final class StateToReproduce {

	public enum ErrorKind {
		EXCEPTION, ROW_NOT_FOUND
	}

	ErrorKind errorKind;

	public final List<Query> statements = new ArrayList<>();
	public String queryString;

	private String databaseName;
	public Map<Column, SQLite3Constant> randomRowValues;

	public String databaseVersion;

	public String values;

	String exception;

	public String queryTargetedTablesString;

	public String queryTargetedColumnsString;

	public SQLite3Expression whereClause;

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

	public SQLite3Expression getWhereClause() {
		return whereClause;
	}

	public Map<Column, SQLite3Constant> getRandomRowValues() {
		return randomRowValues;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("database: " + databaseName + "\n");
		sb.append("version: " + databaseVersion + "\n");
		if (randomRowValues != null) {
			sb.append("expected values: " + randomRowValues + "\n");
		}
		sb.append("-- statements start here\n");
		if (statements != null) {
			sb.append(statements.stream().map(q -> q.getQueryString()).collect(Collectors.joining(";\n")) + "\n");
		}
		if (queryString != null) {
			sb.append(queryString + ";\n");
		}
		return sb.toString();
	}

	public ErrorKind getErrorKind() {
		return errorKind;
	}

	public void setErrorKind(ErrorKind errorKind) {
		this.errorKind = errorKind;
	}

}
