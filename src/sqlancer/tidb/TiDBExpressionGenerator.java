package sqlancer.tidb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public class TiDBExpressionGenerator {
	
	private final TiDBGlobalState globalState;
	private List<MySQLColumn> columns = new ArrayList<>();
	
	public TiDBExpressionGenerator(TiDBGlobalState globalState) {
		this.globalState = globalState;
	}

}
