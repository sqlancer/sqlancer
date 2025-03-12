package sqlancer.mysql;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
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

    private int ref;
    private static final String TRUE_LITERAL = "TRUE";
    private static final String FALSE_LITERAL = "FALSE";
    private static final String NULL_LITERAL = "NULL";
    private static final String UNKNOWN_LITERAL = "UNKNOWN";

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
        switch (s.getFromOptions()) {
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
            throw new AssertionError("Unexpected FROM option");
        }
    appendModifiers(s);
    appendColumns(s);
    appendFromClause(s);
    appendJoins(s);
    appendWhereClause(s);
    appendGroupByClause(s);
    appendOrderByClause(s);
    appendLimitAndOffset(s);
}

private void appendModifiers(MySQLSelect s) {
    String modifiers = s.getModifiers().stream().collect(Collectors.joining(" "));
    if (!modifiers.isEmpty()) {
        sb.append(modifiers).append(" ");
    }
}

    private void appendColumns(MySQLSelect s) {
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getFetchColumns().get(i));
                sb.append(" AS ref").append(ref++);
            }
        }
    }

    private void appendFromClause(MySQLSelect s) {
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
    }

    private void appendJoins(MySQLSelect s) {
        for (MySQLExpression join : s.getJoinList()) {
            visit(join);
        }
    }

    private void appendWhereClause(MySQLSelect s) {
        if (s.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(s.getWhereClause());
        }
private void appendGroupByClause(MySQLSelect s) {
    if (s.getGroupByExpressions() != null && !s.getGroupByExpressions().isEmpty()) {
        sb.append(" GROUP BY ");
        List<MySQLExpression> groupBys = s.getGroupByExpressions();
        for (int i = 0; i < groupBys.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(groupBys.get(i));
        }
    }

    private void appendOrderByClause(MySQLSelect s) {
        if (!s.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            appendExpressionList(s.getOrderByClauses());
        }
    }

    private void appendLimitAndOffset(MySQLSelect s) {
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }
        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    private void appendExpressionList(List<MySQLExpression> expressions) {
        for (int i = 0; i < expressions.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(expressions.get(i));
        }
    }

    @Override
    public void visit(MySQLConstant constant) {
        if (constant.isBoolean()) {
            sb.append(constant.asBoolean() ? TRUE_LITERAL : FALSE_LITERAL);
        } else {
            sb.append(constant.getTextRepresentation());
        }
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(MySQLColumnReference column) {
        sb.append(column.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(MySQLUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(") IS ");
        if (op.isNegated()) {
            sb.append("NOT ");
        }
        switch (op.getOperator()) {
        case IS_FALSE:
            sb.append(FALSE_LITERAL);
            break;
        case IS_NULL:
            sb.append(Randomly.getBoolean() ? UNKNOWN_LITERAL : NULL_LITERAL);
            break;
        case IS_TRUE:
            sb.append(TRUE_LITERAL);
            break;
        default:
            throw new AssertionError("Unexpected operator: " + op.getOperator());
        }
    }

    @Override
    public void visit(MySQLComputableFunction f) {
        sb.append(f.getFunction().getName())
          .append("(");
        MySQLExpression[] args = f.getArguments();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            visit(args[i]);
        }
        sb.append(")");
    }

    @Override
    public void visit(MySQLBinaryLogicalOperation op) {
        appendParenthesizedExpression(op.getLeft());
        sb.append(" ").append(op.getTextRepresentation()).append(" ");
        appendParenthesizedExpression(op.getRight());
    }

    @Override
    public void visit(MySQLBinaryComparisonOperation op) {
        appendParenthesizedExpression(op.getLeft());
        sb.append(" ").append(op.getOp().getTextRepresentation()).append(" ");
        appendParenthesizedExpression(op.getRight());
    }

    private void appendParenthesizedExpression(MySQLExpression expr) {
        sb.append("(");
        visit(expr);
        sb.append(")");
    }

    @Override
    public void visit(MySQLCastOperation op) {
        sb.append("CAST(");
        visit(op.getExpr());
        sb.append(" AS ").append(op.getType()).append(")");
    }

    @Override
    public void visit(MySQLInOperation op) {
        appendParenthesizedExpression(op.getExpr());
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        appendExpressionList(op.getListElements());
        sb.append(")");
    }

    @Override
    public void visit(MySQLBinaryOperation op) {
        appendParenthesizedExpression(op.getLeft());
        sb.append(" ").append(op.getOp().getTextRepresentation()).append(" ");
        appendParenthesizedExpression(op.getRight());
    }

    @Override
    public void visit(MySQLOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ").append(op.getOrder() == MySQLOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(MySQLExists op) {
        sb.append("EXISTS (");
        visit(op.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(MySQLStringExpression op) {
        sb.append(op.getStr());
    }

    @Override
    public void visit(MySQLBetweenOperation op) {
        appendParenthesizedExpression(op.getExpr());
        sb.append(" BETWEEN ");
        appendParenthesizedExpression(op.getLeft());
        sb.append(" AND ");
        appendParenthesizedExpression(op.getRight());
    }

    @Override
    public void visit(MySQLTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(MySQLCollate collate) {
        appendParenthesizedExpression(collate.getExpression());
        sb.append(" ").append(collate.getOperatorRepresentation());
    }

    @Override
    public void visit(MySQLJoin join) {
        sb.append(" ");
        switch (join.getType()) {
        case NATURAL:
            sb.append("NATURAL ");
            break;
        case INNER:
            sb.append("INNER ");
            break;
        case STRAIGHT:
            sb.append("STRAIGHT_");
            break;
        case LEFT:
            sb.append("LEFT ");
            break;
        case RIGHT:
            sb.append("RIGHT ");
            break;
        case CROSS:
            sb.append("CROSS ");
            break;
        default:
            throw new AssertionError("Unexpected join type: " + join.getType());
        }
        sb.append("JOIN ");
        sb.append(join.getTable().getName());
        if (join.getOnClause() != null) {
            sb.append(" ON ");
            visit(join.getOnClause());
        }
    }

    @Override
    public void visit(MySQLText text) {
        sb.append(text.getText());
    }

    @Override
    public void visit(MySQLAggregate aggr) {
        MySQLAggregateFunction func = aggr.getFunc();
        sb.append(func.getName()).append("(");
        
        String option = func.getOption();
        if (option != null) {
            sb.append(option).append(" ");
        }
        
        appendExpressionList(aggr.getExprs());
        sb.append(")");
    }
}
