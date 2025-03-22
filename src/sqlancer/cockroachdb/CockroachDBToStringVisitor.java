package sqlancer.cockroachdb;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.ast.CockroachDBAggregate;
import sqlancer.cockroachdb.ast.CockroachDBAlias;
import sqlancer.cockroachdb.ast.CockroachDBAllOperator;
import sqlancer.cockroachdb.ast.CockroachDBAnyOperator;
import sqlancer.cockroachdb.ast.CockroachDBBetweenOperation;
import sqlancer.cockroachdb.ast.CockroachDBCaseOperation;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBConstant.CockroachDBNullConstant;
import sqlancer.cockroachdb.ast.CockroachDBExists;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBExpressionBag;
import sqlancer.cockroachdb.ast.CockroachDBFunctionCall;
import sqlancer.cockroachdb.ast.CockroachDBInOperation;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBMultiValuedComparison;
import sqlancer.cockroachdb.ast.CockroachDBResultMap;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableAndColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBTypeof;
import sqlancer.cockroachdb.ast.CockroachDBValues;
import sqlancer.cockroachdb.ast.CockroachDBWithClause;
import sqlancer.common.visitor.ToStringVisitor;

public class CockroachDBToStringVisitor extends ToStringVisitor<CockroachDBExpression> implements CockroachDBVisitor {

    @Override
    public void visitSpecific(CockroachDBExpression expr) {
        CockroachDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(CockroachDBConstant c) {
        sb.append(c.toString());
    }

    public String getString() {
        return sb.toString();
    }

    @Override
    public void visit(CockroachDBColumnReference c) {
        if (c.getColumn().getTable() == null) {
            sb.append(c.getColumn().getName());
        } else {
            sb.append(c.getColumn().getFullQualifiedName());
        }
    }

    @Override
    public void visit(CockroachDBFunctionCall call) {
        sb.append(call.getName());
        sb.append("(");
        visit(call.getArguments());
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBInOperation inOp) {
        sb.append("(");
        visit(inOp.getLeft());
        sb.append(") IN (");
        visit(inOp.getRight());
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getType().getStringRepresentation());
        sb.append(" (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBSelect select) {
        if (select.getWithClause() != null) {
            visit(select.getWithClause());
        }
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        } else if (Randomly.getBoolean()) {
            sb.append("ALL ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        if (!select.getFromList().isEmpty()) {
            visit(select.getFromList().stream().map(t -> t).collect(Collectors.toList()));
        }
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        visit(select.getJoinList().stream().map(j -> j).collect(Collectors.toList()));
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (select.getGroupByExpressions() != null && !select.getGroupByExpressions().isEmpty()) {
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

    @Override
    public void visit(CockroachDBCaseOperation cases) {
        sb.append("CASE ");
        for (int i = 0; i < cases.getConditions().size(); i++) {
            CockroachDBExpression predicate = cases.getConditions().get(i);
            CockroachDBExpression then = cases.getThenClauses().get(i);
            sb.append(" WHEN ");
            visit(predicate);
            sb.append(" THEN ");
            visit(then);
            sb.append(" ");
        }
        if (cases.getElseClause() != null) {
            sb.append("ELSE ");
            visit(cases.getElseClause());
            sb.append(" ");
        }
        sb.append("END");
    }

    @Override
    public void visit(CockroachDBJoin join) {
        visit(join.getLeftTable());
        switch (join.getJoinType()) {
        case INNER:
            sb.append(" INNER ");
            potentiallyAddHint(false);
            sb.append("JOIN ");
            visit(join.getRightTable());
            sb.append(" ON ");
            visit(join.getOnCondition());
            break;
        case LEFT:
            sb.append(" LEFT");
            sb.append(" OUTER ");
            potentiallyAddHint(true);
            sb.append("JOIN ");
            visit(join.getRightTable());
            sb.append(" ON ");
            visit(join.getOnCondition());
            break;
        case RIGHT:
            sb.append(" RIGHT");
            sb.append(" OUTER ");
            potentiallyAddHint(true);
            sb.append("JOIN ");
            visit(join.getRightTable());
            sb.append(" ON ");
            visit(join.getOnCondition());
            break;
        case FULL:
            sb.append(" FULL");
            sb.append(" OUTER ");
            potentiallyAddHint(true);
            sb.append("JOIN ");
            visit(join.getRightTable());
            sb.append(" ON ");
            visit(join.getOnCondition());
            break;
        case CROSS:
            sb.append(" CROSS ");
            potentiallyAddHint(false);
            sb.append("JOIN ");
            visit(join.getRightTable());
            break;
        case NATURAL:
            sb.append(" NATURAL ");
            // potentiallyAddHint(false);
            sb.append("JOIN ");
            visit(join.getRightTable());
            break;
        default:
            throw new AssertionError();
        }
    }

    private void potentiallyAddHint(boolean isOuter) {
        if (Randomly.getBoolean()) {
            return;
        } else {
            if (isOuter) {
                sb.append(Randomly.fromOptions("HASH", "MERGE", "LOOKUP"));
            } else {
                sb.append(Randomly.fromOptions("HASH", "MERGE"));
            }
            sb.append(" ");
        }
    }

    @Override
    public void visit(CockroachDBTableReference tableRef) {
        sb.append(tableRef.getTable().getName());
    }

    @Override
    public void visit(CockroachDBAggregate aggr) {
        sb.append(aggr.getFunc().name());
        sb.append("(");
        visit(aggr.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBMultiValuedComparison comp) {
        sb.append("(");
        visit(comp.getLeft());
        sb.append(" ");
        sb.append(comp.getOp().getStringRepr());
        sb.append(" ");
        sb.append(comp.getType());
        sb.append(" (");
        visit(comp.getRight());
        sb.append(")");
        sb.append(")");
    }


    @Override
    public void visit(CockroachDBExists existsExpr) {
        if (existsExpr.getNegated()) {
            sb.append(" NOT");
        }
        sb.append(" EXISTS(");
        visit(existsExpr.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBExpressionBag exprBag) {
        visit(exprBag.getInnerExpr());
    }

    @Override
    public void visit(CockroachDBValues values) {
        LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> vs = values.getValues();
        int size = vs.values().iterator().next().size();
        sb.append("(VALUES ");
        for (int i = 0; i < size; i++) {
            sb.append("(");
            boolean isFirstColumn = true;
            for (CockroachDBColumn c: vs.keySet()) {
                if (!isFirstColumn) {
                    sb.append(", ");
                }
                sb.append(vs.get(c).get(i).toString());
                if (!(vs.get(c).get(i) instanceof CockroachDBNullConstant)) {
                    if (c.getType() != null) {
                        sb.append("::" + c.getType().toString());
                    }
                }
                isFirstColumn = false;
            }
            sb.append(")");
            if (i < size - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBWithClause withClause) {
        sb.append("WITH ");
        visit(withClause.getLeft());
        sb.append(" AS (");
        visit(withClause.getRight());
        sb.append(") ");
    }

    @Override
    public void visit(CockroachDBTableAndColumnReference tableAndColumnReference) {
        CockroachDBTable table = tableAndColumnReference.getTable();
        sb.append(table.getName());
        sb.append("(");
        sb.append(table.getColumnsAsString());
        sb.append(") ");
    }

    @Override
    public void visit(CockroachDBAlias alias) {
        CockroachDBExpression e = alias.getExpression();
        if (e instanceof CockroachDBSelect) {
            sb.append("(");
        }
        visit(e);
        if (e instanceof CockroachDBSelect) {
            sb.append(")");
        }
        sb.append(" AS ");
        sb.append(alias.getAlias());
    }

    @Override
    public void visit(CockroachDBTypeof typeOf) {
        sb.append("pg_typeof(");
        visit(typeOf.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBResultMap expr) {
        // use CASE WHEN to express the constant result of a expression
        LinkedHashMap<CockroachDBColumnReference, List<CockroachDBConstant>> dbstate = expr.getDbStates();
        List<CockroachDBConstant> result = expr.getResult();
        int size = dbstate.values().iterator().next().size();
        if (size == 0) {
            sb.append(" NULL ");
            return;
        }
        sb.append(" CASE ");
        for (int i = 0; i < size; i++) {
            sb.append("WHEN ");
            Boolean isFirstCondition = true;
            for (CockroachDBColumnReference columnRef : dbstate.keySet()) {
                if (!isFirstCondition) {
                    sb.append(" AND ");
                }
                visit(columnRef);
                if (dbstate.get(columnRef).get(i) instanceof CockroachDBNullConstant) {
                    sb.append(" IS NULL");
                } else {
                    sb.append(" = ");
                    visit(dbstate.get(columnRef).get(i));
                    if (!(dbstate.get(columnRef).get(i) instanceof CockroachDBNullConstant)) {
                        if (columnRef.getColumn().getType() != null) {
                            sb.append("::" + columnRef.getColumn().getType().toString());
                        }
                    }
                }
                isFirstCondition = false;
            }
            sb.append(" THEN ");
            visit(result.get(i));
            if (!(result.get(i) instanceof CockroachDBNullConstant)) {
                if (expr.getResultType() != null) {
                    sb.append("::" + expr.getResultType().toString());
                }
            }
            sb.append(" ");
        }
        sb.append("END ");
    }

    @Override
    public void visit(CockroachDBAllOperator allOperation) {
        sb.append("(");
        visit(allOperation.getLeftExpr());
        sb.append(") ");
        sb.append(allOperation.getOperator());
        sb.append(" ALL (");
        // if (allOperation.getRightExpr() instanceof CockroachDBValues) {
        //     sb.append("SELECT ");
        // }
        visit(allOperation.getRightExpr());
        sb.append(")");
    }

    @Override
    public void visit(CockroachDBAnyOperator anyOperation) {
        sb.append("(");
        visit(anyOperation.getLeftExpr());
        sb.append(") ");
        sb.append(anyOperation.getOperator());
        sb.append(" ANY (");
        // if (anyOperation.getRightExpr() instanceof CockroachDBValues) {
        //     sb.append("SELECT ");
        // }
        visit(anyOperation.getRightExpr());
        sb.append(")");
    }
}
