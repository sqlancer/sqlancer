package sqlancer.duckdb;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.duckdb.ast.DuckDBColumnReference;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBConstantWithType;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBExpressionBag;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBResultMap;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTypeCast;
import sqlancer.duckdb.ast.DuckDBTypeofNode;
import sqlancer.duckdb.ast.DuckDBConstant.DuckDBNullConstant;

public class DuckDBToStringVisitor extends NewToStringVisitor<DuckDBExpression> {

    @Override
    public void visitSpecific(DuckDBExpression expr) {
        if (expr instanceof DuckDBConstant) {
            visit((DuckDBConstant) expr);
        } else if (expr instanceof DuckDBSelect) {
            visit((DuckDBSelect) expr);
        } else if (expr instanceof DuckDBJoin) {
            visit((DuckDBJoin) expr);
        } else if (expr instanceof DuckDBConstantWithType) {
            visit((DuckDBConstantWithType) expr);
        } else if (expr instanceof DuckDBResultMap) {
            visit((DuckDBResultMap) expr);
        } else if (expr instanceof DuckDBTypeCast) {
            visit((DuckDBTypeCast) expr);
        } else if (expr instanceof DuckDBTypeofNode) {
            visit((DuckDBTypeofNode) expr);
        } else if (expr instanceof DuckDBExpressionBag) {
            visit((DuckDBExpressionBag) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DuckDBJoin join) {
        visit((TableReferenceNode<DuckDBExpression, DuckDBSchema.DuckDBTable>) join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit((TableReferenceNode<DuckDBExpression, DuckDBSchema.DuckDBTable>) join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(DuckDBConstant constant) {
        sb.append("(");
        sb.append(constant.toString());
        sb.append(")");
    }

    private void visit(DuckDBSelect select) {
        if (select.getWithClause() != null) {
            visit(select.getWithClause());
        }
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        visit(select.getFromList());
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
            visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClauses());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    public void visit(DuckDBConstantWithType expr) {
        sb.append(expr.toString());
    }

    public void visit(DuckDBResultMap expr) {
        // use CASE WHEN to express the constant result of a expression
        LinkedHashMap<DuckDBColumnReference, List<DuckDBExpression>> dbstate = expr.getDbStates();
        List<DuckDBExpression> result = expr.getResult();
        int size = dbstate.values().iterator().next().size();
        if (size == 0) {
            sb.append(" NULL ");
            return;
        }
        sb.append(" CASE ");
        for (int i = 0; i < size; i++) {
            sb.append("WHEN ");
            Boolean isFirstCondition = true;
            for (DuckDBColumnReference columnRef : dbstate.keySet()) {
                if (!isFirstCondition) {
                    sb.append(" AND ");
                }
                visit(columnRef);
                if (dbstate.get(columnRef).get(i) instanceof DuckDBNullConstant) {
                    sb.append(" IS NULL");
                } else if (dbstate.get(columnRef).get(i) instanceof DuckDBConstantWithType) {
                    DuckDBConstantWithType ct = (DuckDBConstantWithType) dbstate.get(columnRef).get(i);
                    if (ct.getConstant() instanceof DuckDBNullConstant) {
                        sb.append(" IS NULL");
                    } else {
                        sb.append(" = ");
                        visit(dbstate.get(columnRef).get(i));
                    }
                } else {
                    sb.append(" = ");
                    visit(dbstate.get(columnRef).get(i));
                }
                isFirstCondition = false;
            }
            sb.append(" THEN ");
            visit(result.get(i));
            sb.append(" ");
        }
        sb.append("END ");
    }

    public void visit(DuckDBTypeCast expr) {
        sb.append("(");
        visit(expr.getExpression());
        sb.append(")::");
        sb.append(expr.getType().toString());
        sb.append(" ");
    }

    public void visit(DuckDBTypeofNode expr) {
        sb.append("typeof(");
        visit(expr.getExpr());
        sb.append(")");
    }

    public void visit(DuckDBExpressionBag expr) {
        visit(expr.getInnerExpr());
    }

    public static String asString(DuckDBExpression expr) {
        DuckDBToStringVisitor visitor = new DuckDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }


}
