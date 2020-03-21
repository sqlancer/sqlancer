package sqlancer.tdengine;

import sqlancer.tdengine.expr.TDEngineBinaryComparisonOperation;
import sqlancer.tdengine.expr.TDEngineColumnName;
import sqlancer.tdengine.expr.TDEngineConstant;
import sqlancer.tdengine.expr.TDEngineExpression;
import sqlancer.tdengine.expr.TDEngineOrderingTerm;
import sqlancer.tdengine.expr.TDEngineSelectStatement;

public abstract class TDEngineVisitor {
	
	
	public abstract void visit(TDEngineConstant c);
	
	public abstract void visit(TDEngineColumnName c);
	
	public abstract void visit(TDEngineSelectStatement s);

	public abstract void visit(TDEngineOrderingTerm s);
	
	public abstract void visit(TDEngineBinaryComparisonOperation s);

	
	public void visit(TDEngineExpression expr) {
		if (expr instanceof TDEngineConstant) {
			visit((TDEngineConstant) expr);
		} else if (expr instanceof TDEngineColumnName) {
			visit((TDEngineColumnName) expr);
		} else if (expr instanceof TDEngineSelectStatement) {
			visit((TDEngineSelectStatement) expr);
		} else if (expr instanceof TDEngineOrderingTerm) {
			visit((TDEngineOrderingTerm) expr);
		} else if (expr instanceof TDEngineBinaryComparisonOperation) {
			visit((TDEngineBinaryComparisonOperation) expr);
		} else {
			throw new AssertionError(expr);
		}
		
	}
	
	public static String asString(TDEngineExpression expr) {
		if (expr == null) {
			throw new AssertionError();
		}
		TDEngineToStringVisitor visitor = new TDEngineToStringVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

	public static String asExpectedValues(TDEngineExpression expr) {
		assert expr != null;
		TDEngineExpectedValueVisitor visitor = new TDEngineExpectedValueVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

}
