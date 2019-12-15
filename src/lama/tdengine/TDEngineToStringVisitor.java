package lama.tdengine;

import lama.tdengine.expr.TDEngineBinaryComparisonOperation;
import lama.tdengine.expr.TDEngineColumnName;
import lama.tdengine.expr.TDEngineConstant;
import lama.tdengine.expr.TDEngineOrderingTerm;
import lama.tdengine.expr.TDEngineSelectStatement;

public class TDEngineToStringVisitor extends TDEngineVisitor {
	
	private final StringBuilder sb = new StringBuilder();

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(TDEngineConstant c) {
		sb.append(c.toString());
	}

	@Override
	public void visit(TDEngineColumnName c) {
		sb.append(c.getColumn().getFullQualifiedName());
	}

	@Override
	public void visit(TDEngineSelectStatement s) {
		sb.append("SELECT ");
		if (s.getColumns() == null) {
			sb.append("*");
		} else {
			for (int i = 0; i < s.getColumns().size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				visit(s.getColumns().get(i));
			}
		}
		sb.append(" FROM ");
		sb.append(s.getTable().getName());
		if (s.getWhereClause() != null) {
			sb.append(" WHERE ");
			visit(s.getWhereClause());
		}
		if (s.getOrderBy() != null) {
			sb.append(" ORDER BY ");
			visit(s.getOrderBy());
		}
		if (s.getLimitClause() != null) {
			sb.append("LIMIT ");
			visit(s.getLimitClause());
			if (s.getOffsetClause() != null) {
				sb.append(", ");
				visit(s.getOffsetClause());
			}
		}
		
	}

	@Override
	public void visit(TDEngineOrderingTerm s) {
		sb.append(" ORDER BY ");
		visit(s.getExpr());
		if (s.getOrder() != null) {
			sb.append(s.getOrder());
		}
	}

	@Override
	public void visit(TDEngineBinaryComparisonOperation s) {
		sb.append("(");
		visit(s.getLeft());
		sb.append(") ");
		sb.append(s.getOp());
		sb.append(" (");
		visit(s.getRight());
		sb.append(") ");
	}

}
