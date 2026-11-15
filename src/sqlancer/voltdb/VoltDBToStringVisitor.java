package sqlancer.voltdb;

import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.voltdb.ast.VoltDBAggregate;
import sqlancer.voltdb.ast.VoltDBCase;
import sqlancer.voltdb.ast.VoltDBConstant;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBJoin;
import sqlancer.voltdb.ast.VoltDBKeyword;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.ast.VoltDBSubquery;
import sqlancer.voltdb.ast.VoltDBWindowFunction;

public class VoltDBToStringVisitor extends NewToStringVisitor<VoltDBExpression> {

    @Override
    public void visitSpecific(VoltDBExpression expr) {
        if (expr instanceof VoltDBConstant || expr instanceof VoltDBKeyword) {
            sb.append(expr.toString());
        } else if (expr instanceof VoltDBSelect) {
            visit((VoltDBSelect) expr);
        } else if (expr instanceof VoltDBJoin) {
            visit((VoltDBJoin) expr);
        } else if (expr instanceof VoltDBCase) {
            visit((VoltDBCase) expr);
        } else if (expr instanceof VoltDBSubquery) {
            visit((VoltDBSubquery) expr);
        } else if (expr instanceof VoltDBWindowFunction) {
            visit((VoltDBWindowFunction) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    // ENG-30506: in a parenthesized AND/OR list, an operand that starts with a function call after an IN predicate
    // crashes VoltDB's parser, e.g. (a IN (1) OR ABS(a) > 5); BETWEEN and IN print their left operand without
    // parentheses, so a function call is always printed in parentheses
    @Override
    public void visit(NewFunctionNode<VoltDBExpression, ?> funcCall) {
        sb.append("(");
        if (funcCall instanceof VoltDBAggregate && funcCall.getArgs().isEmpty()) {
            sb.append(funcCall.getFunc()).append("(*)");
        } else {
            super.visit(funcCall);
        }
        sb.append(")");
    }

    private void visit(VoltDBCase c) {
        sb.append("(CASE");
        for (int i = 0; i < c.getConditions().size(); i++) {
            sb.append(" WHEN ");
            visit(c.getConditions().get(i));
            sb.append(" THEN ");
            visit(c.getResults().get(i));
        }
        if (c.getElseResult() != null) {
            sb.append(" ELSE ");
            visit(c.getElseResult());
        }
        sb.append(" END)");
    }

    private void visit(VoltDBSubquery s) {
        switch (s.getForm()) {
        case EXISTS:
            sb.append("(EXISTS ");
            appendSubquery(s, null);
            sb.append(")");
            break;
        case IN:
            sb.append("(");
            visit(s.getLeft());
            sb.append(s.isNegated() ? " NOT IN " : " IN ");
            appendSubquery(s, s.getOutput());
            sb.append(")");
            break;
        case QUANTIFIED:
            sb.append("(");
            visit(s.getLeft());
            sb.append(s.getOperator().getTextRepresentation()).append(s.isAll() ? "ALL " : "ANY ");
            appendSubquery(s, s.getOutput());
            sb.append(")");
            break;
        case SCALAR:
            appendSubquery(s, s.getOutput());
            break;
        default:
            throw new AssertionError(s.getForm());
        }
    }

    // (SELECT output FROM t AS s [WHERE q]); output null is 1
    private void appendSubquery(VoltDBSubquery s, VoltDBExpression output) {
        sb.append("(SELECT ");
        if (output == null) {
            sb.append('1');
        } else {
            visit(output);
        }
        sb.append(" FROM ").append(s.getTableName()).append(" AS ").append(s.getAlias().getName());
        if (s.getWhere() != null) {
            sb.append(" WHERE ");
            visit(s.getWhere());
        }
        sb.append(')');
    }

    private void visit(VoltDBWindowFunction w) {
        sb.append(w.getFunction()).append('(');
        if (w.getFunction().isAggregate()) {
            if (w.getArgument() == null) {
                sb.append('*');
            } else {
                visit(w.getArgument());
            }
        }
        sb.append(") OVER (");
        if (!w.getPartitionBy().isEmpty()) {
            sb.append("PARTITION BY ");
            visit(w.getPartitionBy());
        }
        if (!w.getOrderBy().isEmpty()) {
            if (!w.getPartitionBy().isEmpty()) {
                sb.append(' ');
            }
            sb.append("ORDER BY ");
            for (int i = 0; i < w.getOrderBy().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(w.getOrderBy().get(i));
                if (w.getDescending().get(i)) {
                    sb.append(" DESC");
                }
            }
        }
        sb.append(')');
    }

    private void visit(VoltDBJoin join) {
        sb.append(' ').append(join.getJoinType()).append(" JOIN ");
        visit((VoltDBExpression) join.getTable());
        sb.append(" ON ");
        visit(join.getOnCondition());
    }

    private void visit(VoltDBSelect select) {
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        // the joins attach to the last table of the FROM list, which is the one their ON conditions reference
        visit(select.getFromList());
        for (VoltDBExpression join : select.getJoinList()) {
            visit(join);
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

    public static String asString(VoltDBExpression expr) {
        VoltDBToStringVisitor visitor = new VoltDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
