package sqlancer.tidb.gen;

import java.sql.SQLException;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBDeleteGenerator {

	public static Query getQuery(TiDBGlobalState globalState) throws SQLException {
		TiDBTable table = globalState.getSchema().getRandomTable();
		TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(table.getColumns());
		StringBuilder sb = new StringBuilder("DELETE FROM ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(TiDBVisitor.asString(gen.generateExpression()));
		}
		return new QueryAdapter(sb.toString());
		
	}
	
}
