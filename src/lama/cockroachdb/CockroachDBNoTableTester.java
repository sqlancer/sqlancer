package lama.cockroachdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lama.IgnoreMeException;
import lama.QueryAdapter;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTable;
import lama.cockroachdb.ast.CockroachDBSelect;
import lama.cockroachdb.ast.CockroachDBTableReference;

public class CockroachDBNoTableTester {

	private CockroachDBGlobalState state;
	private final Set<String> errors = new HashSet<>();

	public CockroachDBNoTableTester(CockroachDBGlobalState state) {
		this.state = state;
		CockroachDBErrors.addExpressionErrors(errors);
		// https://github.com/cockroachdb/cockroach/issues/46122
		errors.add("zero length schema unsupported");
		// https://github.com/cockroachdb/cockroach/issues/46123
		errors.add("input to aggregatorBase is not an execinfra.OpNode");

		errors.add("interface conversion: coldata.column");

		// https://github.com/cockroachdb/cockroach/issues/46187
		errors.add("internal error: node lookup-join with MaxCost added to the memo");
		
		errors.add("invalid batch limit");
	}

	public void check() throws SQLException {
		CockroachDBSelect select = CockroachDBRandomQuerySynthesizer.generateSelect(state, 1);
		QueryAdapter query = new QueryAdapter(CockroachDBVisitor.asString(select), errors);
		List<String> resultSet = new ArrayList<>();
		List<String> secondResultSet = new ArrayList<>();
		state.getState().queryString = query.getQueryString();
		// https://github.com/cockroachdb/cockroach/issues/46196
		if (query.getQueryString().contains("MAX") || query.getQueryString().contains("MIN") || query.getQueryString().contains("BOOL_AND")  || query.getQueryString().contains("BOOL_OR" )) {
			throw new IgnoreMeException();
		}
		try (ResultSet result = query.executeAndGet(state.getConnection())) {
			if (result == null) {
				throw new IgnoreMeException();
			}
			if (!result.next()) {
				throw new IgnoreMeException();
			}
			while (result.next()) {
				resultSet.add(result.getString(1));
			}
		}
		List<CockroachDBTableReference> fromTables = getNewFromTables(select, state.getConnection());
		select.setFromTables(fromTables);

		QueryAdapter noTableQuery = new QueryAdapter(CockroachDBVisitor.asString(select), errors);
		state.getState().queryString = query.getQueryString() + ";\n" + noTableQuery.getQueryString() + ";\n";
		try (ResultSet result = noTableQuery.executeAndGet(state.getConnection())) {
			if (result == null) {
				throw new IgnoreMeException();
			}
			if (!result.next()) {
				throw new IgnoreMeException();
			}
			while (result.next()) {
				secondResultSet.add(result.getString(1));
			}
		}
		if (resultSet.size() != secondResultSet.size()) {
			throw new AssertionError(resultSet.size() + " " + secondResultSet.size());
		}

	}

	private List<CockroachDBTableReference> getNewFromTables(CockroachDBSelect select, Connection con)
			throws SQLException {
		List<CockroachDBTableReference> fromTables = select.getFromTables();
		List<CockroachDBTableReference> newTables = new ArrayList<>();
		for (CockroachDBTableReference t : fromTables) {
			CockroachDBTable table = t.getTable();
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			sb.append("VALUES ");
			QueryAdapter q = new QueryAdapter("SELECT " + table.getColumnsAsString() + " FROM " + table.getName(),
					errors);
			int j = 0;
			try (ResultSet rs = q.executeAndGet(con)) {
				if (rs == null) {
					throw new IgnoreMeException();
				}
				while (rs.next()) {
					if (j++ != 0) {
						sb.append(", ");
					}
					sb.append("(");
					int i = 0;
					for (CockroachDBColumn c : table.getColumns()) {
						if (i++ != 0) {
							sb.append(", ");
						}
						if (rs.getString(i) == null) {
							sb.append("NULL");
						} else {
							switch (c.getColumnType().getPrimitiveDataType()) {
							case INT:
								sb.append(rs.getLong(i));
								break;
							case FLOAT:
								sb.append(rs.getFloat(i));
								break;
							default:
								throw new IgnoreMeException();
							}
						}
					}
					sb.append(")");
				}
			}
			if (j == 0) {
				throw new IgnoreMeException();
			}
			sb.append(")");
			sb.append(" ");
			sb.append(table.getName());
			sb.append("(");
			sb.append(table.getColumnsAsString(c -> c.getName()));
			sb.append(")");
			newTables.add(new CockroachDBTableReference(
					new CockroachDBTable(sb.toString(), Collections.emptyList(), Collections.emptyList(), false)));
		}
		return newTables;
	}

}
