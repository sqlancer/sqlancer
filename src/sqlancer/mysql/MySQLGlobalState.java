
package sqlancer.mysql;

import sqlancer.GlobalState;

public class MySQLGlobalState extends GlobalState {
	
	private MySQLSchema schema;

	public void setSchema(MySQLSchema schema) {
		this.schema = schema;
	}

	public MySQLSchema getSchema() {
		return schema;
	}

}
