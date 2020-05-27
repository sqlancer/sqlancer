package sqlancer.mariadb.ast;

import java.util.List;
import java.util.stream.Collectors;

public class MariaDBStringVisitor extends MariaDBVisitor {

    private final StringBuilder sb = new StringBuilder();

    @Override
    public void visit(MariaDBConstant c) {
        sb.append(c.toString());
    }

    public String getString() {
        return sb.toString();
    }

    @Override
    public void visit(MariaDBPostfixUnaryOperation op) {
        sb.append("(");
        visit(op.getRandomWhereCondition());
        sb.append(" ");
        sb.append(op.getOperator().getTextRepresentation());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBColumnName c) {
        sb.append(c.getColumn().getName());
    }

    @Override
    public void visit(MariaDBSelectStatement s) {
        sb.append("SELECT ");
        int i = 0;
        for (MariaDBExpression column : s.getColumns()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(column);
        }
        sb.append(" FROM ");
        sb.append(s.getTables().stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        if (s.getWhereCondition() != null) {
            sb.append(" WHERE ");
            visit(s.getWhereCondition());
        }
        if (s.getGroupBys().size() != 0) {
            sb.append(" GROUP BY");
            for (i = 0; i < s.getGroupBys().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getGroupBys().get(i));
            }
        }
    }

    @Override
    public void visit(MariaDBText t) {
        if (t.isPrefix()) {
            sb.append(t.getText());
            visit(t.getExpr());
        } else {
            visit(t.getExpr());
            sb.append(t.getText());
        }
    }

    @Override
    public void visit(MariaDBAggregate aggr) {
        sb.append(aggr.getAggr());
        sb.append("(");
        visit(aggr.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBBinaryOperator comp) {
        sb.append("(");
        visit(comp.getLeft());
        sb.append(" ");
        sb.append(comp.getOp().getTextRepresentation());
        sb.append(" ");
        visit(comp.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBUnaryPrefixOperation op) {
        sb.append("(");
        sb.append(op.getOp().textRepresentation);
        sb.append(" ");
        visit(op.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBFunction func) {
        sb.append(func.getFunc().getFunctionName());
        sb.append("(");
        for (int i = 0; i < func.getArgs().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(func.getArgs().get(i));
        }
        sb.append(")");

    }

    @Override
    public void visit(MariaDBInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        if (op.isNegated()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        visitList(op.getList());
        sb.append("))");
    }

    private void visitList(List<MariaDBExpression> list) {
        for (int i = 0; i < list.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(list.get(i));
        }
    }

}
