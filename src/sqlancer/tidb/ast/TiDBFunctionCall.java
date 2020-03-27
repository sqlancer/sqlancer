package sqlancer.tidb.ast;

import java.util.List;

import sqlancer.Randomly;

public class TiDBFunctionCall implements TiDBExpression {

	private TiDBFunction function;
	private List<TiDBExpression> args;

	public static enum TiDBFunction {
		
		// https://pingcap.github.io/docs/stable/reference/sql/functions-and-operators/bit-functions-and-operators/
		BIT_COUNT(1),
		
		// https://pingcap.github.io/docs/stable/reference/sql/functions-and-operators/information-functions/
		CONNECTION_ID(0),
//		CURRENT_USER(0), https://github.com/pingcap/tidb/issues/15789
		DATABASE(0),
//		FOUND_ROWS(0), <-- non-deterministic
//		LAST_INSERT_ID(0), <-- non-deterministic
//		ROW_COUNT(0),  <-- non-deterministic
//		SCHEMA(0),  https://github.com/pingcap/tidb/issues/15789
//		SESSION_USER(0),  https://github.com/pingcap/tidb/issues/15789
//		SYSTEM_USER(0),  https://github.com/pingcap/tidb/issues/15789
//		USER(0),  https://github.com/pingcap/tidb/issues/15789
//		VERSION(0),  https://github.com/pingcap/tidb/issues/15789
		
		TIDB_VERSION(0),
		
		IF(3),
		IFNULL(2),
		NULLIF(2),
		
		ASCII(1),
		BIN(1),
		BIT_LENGTH(1),
		// https://github.com/pingcap/tidb/issues/15789
//		CHAR(1),
		CHAR_LENGTH(1),
		CHARACTER_LENGTH(1),
//		CONCAT(1, true),
//		CONCAT_WS(2, true),
		ELT(2, true),
		EXPORT_SET(0) {
			@Override
			public int getNrArgs() {
				return Randomly.fromOptions(3, 4, 5);
			}
		}
		;
		
		private int nrArgs;
		private boolean isVariadic;
		
		TiDBFunction(int nrArgs) {
			this.nrArgs = nrArgs;
		}
	
		TiDBFunction(int nrArgs, boolean isVariadic) {
			this.nrArgs = nrArgs;
			this.isVariadic = true;
		}
		
		public static TiDBFunction getRandom() {
			return Randomly.fromOptions(values());
		}
		
		public int getNrArgs() {
			return nrArgs + (isVariadic() ? Randomly.smallNumber() : 0);
		}
		
		public boolean isVariadic() {
			return isVariadic;
		}
		
	}
	
	public TiDBFunctionCall(TiDBFunction function, List<TiDBExpression> args) {
		this.function = function;
		this.args = args;
	}
	
	public List<TiDBExpression> getArgs() {
		return args;
	}
	
	public TiDBFunction getFunction() {
		return function;
	}
	
}
