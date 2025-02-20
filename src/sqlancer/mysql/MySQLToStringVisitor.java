package sqlancer.mysql;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLAggregate.MySQLAggregateFunction;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLCollate;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLOrderByTerm;
import sqlancer.mysql.ast.MySQLOrderByTerm.MySQLOrder;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLText;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;

public class MySQLToStringVisitor extends ToStringVisitor<MySQLExpression> implements MySQLVisitor {

    int ref;

    @Override
    public void visitSpecific(MySQLExpression expr) {
        MySQLVisitor.super.visit(expr);
    }

    @Override
    public void visit(MySQLSelect s) {
        sb.append("SELECT ");
        if (s.getHint() != null) {
            sb.append("/*+ ");
            visit(s.getHint());
            sb.append("*/ ");
        }
        visitSelectOption(s);
        sb.append(s.getModifiers().stream().collect(Collectors.joining(" ")));
        if (s.getModifiers().size() > 0) {
            sb.append(" ");
        }
        visitColumns(s);
        visitFromClause(s);
        visitJoinClauses(s);
        visitWhereClause(s);
        visitGroupByClause(s);
        visitOrderByClause(s);
        visitLimitClause(s);
        visitOffsetClause(s);
    }

    @Override
    public void visitSelectOption(SelectBase<MySQLExpression> s) {
        switch (((MySQLSelect)s).getFromOptions()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            break;
        case ALL:
            sb.append(Randomly.fromOptions("ALL ", ""));
            break;
        case DISTINCTROW:
            sb.append("DISTINCTROW ");
            break;
        default:
            throw new AssertionError();
        }
    }

    @Override
    public void visitColumns(SelectBase<MySQLExpression> s) {
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getFetchColumns().get(i));
                // MySQL does not allow duplicate column names
                sb.append(" AS ");
                sb.append("ref");
                sb.append(ref++);
            }
        }
    }

    @Override
    public void visitFromClause(SelectBase<MySQLExpression> s) {
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
    }

    @Override
    public void visitJoinClauses(SelectBase<MySQLExpression> s) {
        for (MySQLExpression j : s.getJoinList()) {
            visit(j);
        }
    }

    @Override
    public void visitGroupByClause(SelectBase<MySQLExpression> s) {
        if (s.getGroupByExpressions() != null && s.getGroupByExpressions().size() > 0) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<MySQLExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }
    }

    @Override
    public void visitOrderByClause(SelectBase<MySQLExpression> s) {
        if (!s.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            List<MySQLExpression> orderBys = s.getOrderByClauses();
            for (int i = 0; i < orderBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getOrderByClauses().get(i));
            }
        }
    }

    @Override
    public void visit(MySQLConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    protected MySQLExpression getJoinOnClause(JoinBase<MySQLExpression> join) {
        return null;
    }

    @Override
    protected MySQLExpression getJoinTableReference(JoinBase<MySQLExpression> join) {
        return null;
    }

    @Override
    public void visit(MySQLColumnReference column) {
        sb.append(column.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(MySQLUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" IS ");
        if (op.isNegated()) {
            sb.append("NOT ");
        }
        switch (op.getOperator()) {
        case IS_FALSE:
            sb.append("FALSE");
            break;
        case IS_NULL:
            if (Randomly.getBoolean()) {
                sb.append("UNKNOWN");
            } else {
                sb.append("NULL");
            }
            break;
        case IS_TRUE:
            sb.append("TRUE");
            break;
        default:
            throw new AssertionError(op);
        }
    }

    @Override
    public void visit(MySQLComputableFunction f) {
        sb.append(f.getFunction().getName());
        sb.append("(");
        for (int i = 0; i < f.getArguments().length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(f.getArguments()[i]);
        }
        sb.append(")");
    }

    @Override
    public void visit(MySQLBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getTextRepresentation());
        sb.append(" ");
        sb.append("(");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MySQLBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MySQLCastOperation op) {
        sb.append("CAST(");
        visit(op.getExpr());
        sb.append(" AS ");
        sb.append(op.getType());
        sb.append(")");
    }

    @Override
    public void visit(MySQLInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN ");
        sb.append("(");
        for (int i = 0; i < op.getListElements().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(op.getListElements().get(i));
        }
        sb.append(")");
    }

    @Override
    public void visit(MySQLBinaryOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MySQLOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == MySQLOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(MySQLExists op) {
        sb.append(" EXISTS (");
        visit(op.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(MySQLStringExpression op) {
        sb.append(op.getStr());
    }

    @Override
    public void visit(MySQLBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MySQLTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(MySQLCollate collate) {
        sb.append("(");
        visit(collate.getExpression());
        sb.append(" ");
        sb.append(collate.getOperatorRepresentation());
        sb.append(")");
    }

    @Override
    public void visit(MySQLJoin join) {
        visitBasicJoinType(join);
        sb.append("JOIN ");
        sb.append(join.getTable().getName());
        visitOnClauses(join);
    }

    @Override
    public void visit(MySQLText text) {
        sb.append(text.getText());
    }

    @Override
    public void visit(MySQLAggregate aggr) {
        MySQLAggregateFunction func = aggr.getFunc();
        String option = func.getOption();
        List<MySQLExpression> exprs = aggr.getExprs();

        sb.append(func.getName());
        sb.append("(");
        if (option != null) {
            sb.append(option);
            sb.append(" ");
        }
        for (int i = 0; i < exprs.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(exprs.get(i));
        }
        sb.append(")");
    }
}
