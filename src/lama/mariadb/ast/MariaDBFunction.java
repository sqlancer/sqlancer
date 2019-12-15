package lama.mariadb.ast;

import java.util.List;

public class MariaDBFunction extends MariaDBExpression {

	private MariaDBFunctionName func;
	private List<MariaDBExpression> args;

	public MariaDBFunction(MariaDBFunctionName func, List<MariaDBExpression> args) {
		this.func = func;
		this.args = args;
	}
	
	public MariaDBFunctionName getFunc() {
		return func;
	}
	
	public List<MariaDBExpression> getArgs() {
		return args;
	}
	
}
