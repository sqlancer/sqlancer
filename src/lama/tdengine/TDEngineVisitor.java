package lama.tdengine;

import lama.tdengine.expr.TDEngineBinaryComparisonOperation;
import lama.tdengine.expr.TDEngineColumnName;
import lama.tdengine.expr.TDEngineConstant;
import lama.tdengine.expr.TDEngineExpression;
import lama.tdengine.expr.TDEngineOrderingTerm;
import lama.tdengine.expr.TDEngineSelectStatement;

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
