package sqlancer.mysql;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLCompositeDataType;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLAlias;
import sqlancer.mysql.ast.MySQLAllOperator;
import sqlancer.mysql.ast.MySQLAnyOperator;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLCollate;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLConstant.MySQLNullConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLExpressionBag;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLOrderByTerm;
import sqlancer.mysql.ast.MySQLResultMap;
import sqlancer.mysql.ast.MySQLOrderByTerm.MySQLOrder;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLTableAndColumnReference;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLText;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLValues;
import sqlancer.mysql.ast.MySQLValuesRow;
import sqlancer.mysql.ast.MySQLWithClause;

public class MySQLToStringVisitor extends ToStringVisitor<MySQLExpression> implements MySQLVisitor {

    int ref;

    @Override
    public void visitSpecific(MySQLExpression expr) {
        MySQLVisitor.super.visit(expr);
    }

    @Override
    public void visit(MySQLSelect s) {
        if (s.getWithClause() != null) {
            visit(s.getWithClause());
            sb.append(" ");
        }
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
            throw new AssertionError();
        }
        sb.append(s.getModifiers().stream().collect(Collectors.joining(" ")));
        if (s.getModifiers().size() > 0) {
            sb.append(" ");
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getFetchColumns().get(i));
                // MySQL does not allow duplicate column names
                if (!(s.getFetchColumns().get(i) instanceof MySQLAlias)) {
                    sb.append(" AS ");
                    sb.append("ref");
                    sb.append(ref++);
                }
            }
        }
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
        for (MySQLExpression j : s.getJoinList()) {
            visit(j);
        }

        if (s.getWhereClause() != null) {
            MySQLExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
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
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
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
        if(op.isNegated()) {
            sb.append(" NOT");
        }
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
            throw new AssertionError(join.getType());
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
    public void visit(MySQLExpressionBag bag) {
        visit(bag.getInnerExpr());
    }

    @Override
    public void visit(MySQLTableAndColumnReference tAndCRef) {
        MySQLTable table = tAndCRef.getTable();
        sb.append(table.getName());
        sb.append("(");
        sb.append(table.getColumnsAsString());
        sb.append(") ");
    }

    @Override
    public void visit(MySQLValues values) {
        LinkedHashMap<MySQLColumn, List<MySQLConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        // sb.append("VALUES ");
        // sb.append("(");
        for (int i = 0; i < size; i++) {
            sb.append("(");
            Boolean isFirstColumn = true;
            for (MySQLColumn name : vs.keySet()) {
                if (!isFirstColumn) {
                    sb.append(", ");
                }
                visit(vs.get(name).get(i));
                isFirstColumn = false;
            }
            sb.append(")");
            if (i < size - 1) {
                sb.append(", ");
            }
        }
    }

    @Override
    public void visit(MySQLValuesRow values) {
        // https://database.guide/values-statement-in-mysql/
        LinkedHashMap<MySQLColumn, List<MySQLConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        sb.append("(VALUES ");
        for (int i = 0; i < size; i++) {
            sb.append("ROW(");
            Boolean isFirstColumn = true;
            for (MySQLColumn name : vs.keySet()) {
                if (!isFirstColumn) {
                    sb.append(", ");
                }
                visit(vs.get(name).get(i));
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
    public void visit(MySQLWithClause withClause) {
        sb.append("WITH ");
        visit(withClause.getLeft());
        sb.append(" AS (");
        visit(withClause.getRight());
        sb.append(") ");
    }

    @Override
    public void visit(MySQLResultMap tableSummary) {
        // we use CASE WHEN THEN END here
        LinkedHashMap<MySQLColumnReference, List<MySQLConstant>> vs = tableSummary.getDbStates();
        List<MySQLConstant> results = tableSummary.getResult();
        HashMap<MySQLColumnReference, MySQLCompositeDataType> columnType = tableSummary.getColumnType();

        int size = vs.get(vs.keySet().iterator().next()).size();
        if (size == 0) {
            sb.append("(");
            Boolean isFirstColumn = true;
            for (MySQLColumnReference tr: vs.keySet()) {
                if (!isFirstColumn) {
                    sb.append(" AND ");
                }
                visit(tr);
                sb.append(" IS NULL");
                isFirstColumn = false;
            }
            sb.append(")");
            return;
        }

        sb.append(" CASE ");
        for (int i = 0; i < size; i++) {
            sb.append("WHEN ");
            Boolean isFirstColumn = true;
            for (MySQLColumnReference tr: vs.keySet()) {
                if (!isFirstColumn) {
                    sb.append(" AND ");
                }
                visit(tr);
                if (vs.get(tr).get(i) instanceof MySQLNullConstant) {
                    sb.append(" IS NULL");
                } else {
                    sb.append(" = ");
                    if (columnType != null) {
                        sb.append("CONVERT(");
                    }
                    sb.append(vs.get(tr).get(i).toString());
                    if (columnType != null) {
                        sb.append(", " + columnType.get(tr).toString().replaceAll("'", "").replaceAll("\"", "") + ")");
                    }
                }
                isFirstColumn = false;
            }
            sb.append(" THEN ");
            sb.append("CONVERT(");
            visit(results.get(i));
            sb.append(", " + tableSummary.getResultType().toString().replaceAll("'", "").replaceAll("\"", "") + ")");
            sb.append(" ");
        }
        sb.append("END ");
    }

    @Override
    public void visit(MySQLAllOperator allOperation) {
        sb.append("(");
        visit(allOperation.getLeftExpr());
        sb.append(") ");
        sb.append(allOperation.getOperator());
        sb.append(" ALL (");
        if (allOperation.getRightExpr() instanceof MySQLValues) {
            MySQLValues values = (MySQLValues) allOperation.getRightExpr();
            LinkedHashMap<MySQLColumn, List<MySQLConstant>> vs = values.getValues();
            int size = vs.get(vs.keySet().iterator().next()).size();
            for (int i = 0; i < size; i++) {
                if (i != 0) {
                    sb.append(" UNION SELECT ");
                } else {
                    sb.append(" SELECT ");
                }
                Boolean isFirstColumn = true;
                for (MySQLColumn name : vs.keySet()) {
                    if (!isFirstColumn) {
                        sb.append(", ");
                    }
                    visit(vs.get(name).get(i));
                    isFirstColumn = false;
                }
            }
        } else {
            visit(allOperation.getRightExpr());
        }
        sb.append(")");
    }

    @Override
    public void visit(MySQLAnyOperator anyOperation) {
        sb.append("(");
        visit(anyOperation.getLeftExpr());
        sb.append(") ");
        sb.append(anyOperation.getOperator());
        sb.append(" ANY (");
        if (anyOperation.getRightExpr() instanceof MySQLValues) {
            MySQLValues values = (MySQLValues) anyOperation.getRightExpr();
            LinkedHashMap<MySQLColumn, List<MySQLConstant>> vs = values.getValues();
            int size = vs.get(vs.keySet().iterator().next()).size();
            for (int i = 0; i < size; i++) {
                if (i != 0) {
                    sb.append(" UNION SELECT ");
                } else {
                    sb.append(" SELECT ");
                }
                Boolean isFirstColumn = true;
                for (MySQLColumn name : vs.keySet()) {
                    if (!isFirstColumn) {
                        sb.append(", ");
                    }
                    visit(vs.get(name).get(i));
                    isFirstColumn = false;
                }
            }
        } else {
            visit(anyOperation.getRightExpr());
        }
        sb.append(")");
    }

    @Override
    public void visit(MySQLAlias alias) {
        MySQLExpression e = alias.getExpression();
        if (e instanceof MySQLSelect) {
            sb.append("(");
        }
        visit(e);
        if (e instanceof MySQLSelect) {
            sb.append(")");
        }
        sb.append(" AS ");
        sb.append(alias.getAlias());
    }

    @Override
    public void visit(MySQLAggregate aggr) {
        sb.append(aggr.getFunction());
        sb.append("(");
        visit(aggr.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(MySQLCompositeDataType type) {
        sb.append(type.toString());
    }
}
