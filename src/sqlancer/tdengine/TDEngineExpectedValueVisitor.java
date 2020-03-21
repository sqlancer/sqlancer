package sqlancer.tdengine;

import sqlancer.tdengine.expr.TDEngineBinaryComparisonOperation;
import sqlancer.tdengine.expr.TDEngineColumnName;
import sqlancer.tdengine.expr.TDEngineConstant;
import sqlancer.tdengine.expr.TDEngineExpression;
import sqlancer.tdengine.expr.TDEngineOrderingTerm;
import sqlancer.tdengine.expr.TDEngineSelectStatement;

public class TDEngineExpectedValueVisitor extends TDEngineVisitor {
	
	private final StringBuilder sb = new StringBuilder();
	private int nrTabs = 0;

	private void print(TDEngineExpression expr) {
		TDEngineToStringVisitor v = new TDEngineToStringVisitor();
		v.visit(expr);
		for (int i = 0; i < nrTabs; i++) {
			sb.append("\t");
		}
		sb.append(v.get());
		sb.append(" -- " + expr.getExpectedValue());
		sb.append("\n");
	}

	@Override
	public void visit(TDEngineConstant c) {
		print(c);
	}

	@Override
	public void visit(TDEngineColumnName c) {
		print(c);
	}

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(TDEngineSelectStatement s) {
		visit(s.getWhereClause());
	}

	@Override
	public void visit(TDEngineOrderingTerm s) {
	}

	@Override
	public void visit(TDEngineBinaryComparisonOperation s) {
		print(s);
		visit(s.getLeft());
		visit(s.getRight());
	}

}
