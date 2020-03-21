package sqlancer.cockroachdb.ast;

import java.util.List;

import sqlancer.cockroachdb.CockroachDBFunction;

public class CockroachDBFunctionCall extends CockroachDBExpression {

	private final CockroachDBFunction function;
	private final List<CockroachDBExpression> arguments;

	public CockroachDBFunctionCall(CockroachDBFunction function, List<CockroachDBExpression> arguments) {
		this.function = function;
		this.arguments = arguments;
	}

	public List<CockroachDBExpression> getArguments() {
		return arguments;
	}
	
	public CockroachDBFunction getFunction() {
		return function;
	}

	public String getName() {
		return function.getFunctionName();
	}
	
}
