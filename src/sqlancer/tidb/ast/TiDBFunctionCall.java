package sqlancer.tidb.ast;

import java.util.List;

import sqlancer.Randomly;

public class TiDBFunctionCall implements TiDBExpression {

	private TiDBFunction function;
	private List<TiDBExpression> args;

	public static enum TiDBFunction {
		
		
		TIDB_VERSION(0),
		
		IF(3),
		IFNULL(2),
		NULLIF(2),
		
		ASCII(1),
		BIN(1),
		BIT_LENGTH(1),
		CHAR(1),
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
