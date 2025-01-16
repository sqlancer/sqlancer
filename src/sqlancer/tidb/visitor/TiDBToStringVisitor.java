package sqlancer.tidb.visitor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBCompositeDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.ast.TiDBAggregate;
import sqlancer.tidb.ast.TiDBAlias;
import sqlancer.tidb.ast.TiDBAllOperator;
import sqlancer.tidb.ast.TiDBAnyOperator;
import sqlancer.tidb.ast.TiDBCase;
import sqlancer.tidb.ast.TiDBCastOperation;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBConstant.TiDBNullConstant;
import sqlancer.tidb.ast.TiDBExists;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBExpressionBag;
import sqlancer.tidb.ast.TiDBFunctionCall;
import sqlancer.tidb.ast.TiDBInOperator;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBResultMap;
import sqlancer.tidb.ast.TiDBJoin.JoinType;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableAndColumnReference;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBText;
import sqlancer.tidb.ast.TiDBValues;
import sqlancer.tidb.ast.TiDBValuesRow;
import sqlancer.tidb.ast.TiDBWithClasure;

public class TiDBToStringVisitor extends ToStringVisitor<TiDBExpression> implements TiDBVisitor {

    @Override
    public void visitSpecific(TiDBExpression expr) {
        TiDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(TiDBConstant c) {
        sb.append(c.toString());
    }

    public String getString() {
        return sb.toString();
    }

    @Override
    public void visit(TiDBColumnReference c) {
        if (c.getColumn().getTable() == null) {
            sb.append(c.getColumn().getName());
        } else {
            sb.append(c.getColumn().getFullQualifiedName());
        }
    }

    @Override
    public void visit(TiDBTableReference expr) {
        sb.append(expr.getTable().getName());
    }

    @Override
    public void visit(TiDBSelect select) {
        if (select.getWithClause() != null) {
            visit(select.getWithClause());
            sb.append(" ");
        }
        sb.append("SELECT ");
        if (select.getHint() != null) {
            sb.append("/*+ ");
            visit(select.getHint());
            sb.append("*/");
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
    }

    @Override
    public void visit(TiDBFunctionCall call) {
        sb.append(call.getFunction());
        sb.append("(");
        visit(call.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(TiDBJoin join) {
        sb.append(" ");
        visit(join.getLeftTable());
        sb.append(" ");
        switch (join.getJoinType()) {
        case INNER:
            sb.append("INNER ");
            sb.append("JOIN ");
            break;
        case LEFT:
            sb.append("LEFT ");
            if (Randomly.getBoolean()) {
                sb.append(" OUTER ");
            }
            sb.append("JOIN ");
            break;
        case RIGHT:
            sb.append("RIGHT ");
            if (Randomly.getBoolean()) {
                sb.append(" OUTER ");
            }
            sb.append("JOIN ");
            break;
        case STRAIGHT:
            sb.append("STRAIGHT_JOIN ");
            break;
        case NATURAL:
            sb.append("NATURAL ");
            switch (join.getNaturalJoinType()) {
            case INNER:
                break;
            case LEFT:
                sb.append("LEFT ");
                break;
            case RIGHT:
                sb.append("RIGHT ");
                break;
            default:
                throw new AssertionError();
            }
            sb.append("JOIN ");
            break;
        case CROSS:
            sb.append("CROSS JOIN ");
            break;
        default:
            throw new AssertionError();
        }
        visit(join.getRightTable());
        if (join.getOnCondition() != null && join.getJoinType() != JoinType.NATURAL) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    @Override
    public void visit(TiDBText text) {
        sb.append(text.getText());
    }

    @Override
    public void visit(TiDBAggregate aggr) {
        sb.append(aggr.getFunction());
        sb.append("(");
        visit(aggr.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(TiDBCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpr());
        sb.append(" AS ");
        sb.append(cast.getType());
        sb.append(")");
    }

    @Override
    public void visit(TiDBCase op) {
        sb.append("(CASE ");
        visit(op.getSwitchCondition());
        for (int i = 0; i < op.getConditions().size(); i++) {
            sb.append(" WHEN ");
            visit(op.getConditions().get(i));
            sb.append(" THEN ");
            visit(op.getExpressions().get(i));
        }
        if (op.getElseExpr() != null) {
            sb.append(" ELSE ");
            visit(op.getElseExpr());
        }
        sb.append(" END )");
    }


    // CODDTest
    @Override
    public void visit(TiDBAlias alias) {
        TiDBExpression e = alias.getExpression();
        if (e instanceof TiDBSelect) {
            sb.append("(");
        }
        visit(e);
        if (e instanceof TiDBSelect) {
            sb.append(")");
        }
        sb.append(" AS ");
        sb.append(alias.getAlias());
    }

    @Override
    public void visit(TiDBExists exists) {
        if (exists.getNegated()) {
            sb.append(" NOT");
        }
        sb.append(" EXISTS(");
        visit(exists.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(TiDBExpressionBag exprBag) {
        visit(exprBag.getInnerExpr());
    }

    @Override
    public void visit(TiDBInOperator inOperation) {
        sb.append(" ");
        visit(inOperation.getLeft());

        sb.append(" IN ");
        sb.append("(");
        visit(inOperation.getRight());
        sb.append(")");
    }
    @Override
    public void visit(TiDBTableAndColumnReference tAndCRef) {
        TiDBTable table = tAndCRef.getTable();
        sb.append(table.getName());
        sb.append("(");
        sb.append(table.getColumnsAsString());
        sb.append(") ");
    }

    @Override
    public void visit(TiDBValues values) {
        LinkedHashMap<TiDBColumn, List<TiDBConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        // sb.append("VALUES ");
        // sb.append("(");
        for (int i = 0; i < size; i++) {
            sb.append("(");
            for (TiDBColumn name : vs.keySet()) {
                visit(vs.get(name).get(i));
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.append("), ");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
    }

    @Override
    public void visit(TiDBValuesRow values) {
        // https://database.guide/values-statement-in-mysql/
        LinkedHashMap<TiDBColumn, List<TiDBConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        sb.append("(VALUES ");
        for (int i = 0; i < size; i++) {
            sb.append("ROW(");
            for (TiDBColumn name : vs.keySet()) {
                visit(vs.get(name).get(i));
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.append("), ");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
    }

    @Override
    public void visit(TiDBWithClasure withClasure) {
        sb.append("WITH ");
        visit(withClasure.getLeft());
        sb.append(" AS (");
        visit(withClasure.getRight());
        sb.append(") ");
    }

    @Override
    public void visit(TiDBResultMap tableSummary) {
        // we use CASE WHEN THEN END here
        LinkedHashMap<TiDBColumnReference, List<TiDBConstant>> vs = tableSummary.getDbStates();
        List<TiDBConstant> results = tableSummary.getResult();

        int size = vs.get(vs.keySet().iterator().next()).size();
        if (size == 0) {
            sb.append("(");
            for (TiDBColumnReference tr: vs.keySet()) {
                visit(tr);
                sb.append(" IS NULL AND ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return;
        }

        sb.append(" CASE ");
        for (int i = 0; i < size; i++) {
            sb.append("WHEN ");
            for (TiDBColumnReference tr: vs.keySet()) {
                visit(tr);
                if (vs.get(tr).get(i) instanceof TiDBNullConstant) {
                    sb.append(" IS NULL");
                } else {
                    sb.append(" = ");
                    sb.append(vs.get(tr).get(i).toString());
                    // if (values.getColumns().get(j).getType() != null) {
                    //     sb.append("::" + values.getColumns().get(j).getType().toString());
                    // }
                }
                sb.append(" AND ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.append("THEN ");
            visit(results.get(i));
            // sb.append("::" + summaryDataType.toString() + " ");
            sb.append(" ");
        }
        sb.append("END ");
    }

    @Override
    public void visit(TiDBAllOperator allOperation) {
        sb.append("(");
        visit(allOperation.getLeftExpr());
        sb.append(") ");
        sb.append(allOperation.getOperator());
        sb.append(" ALL (");
        if (allOperation.getRightExpr() instanceof TiDBValues) {
            TiDBValues values = (TiDBValues) allOperation.getRightExpr();
            LinkedHashMap<TiDBColumn, List<TiDBConstant>> vs = values.getValues();
            int size = vs.get(vs.keySet().iterator().next()).size();
            for (int i = 0; i < size; i++) {
                if (i != 0) {
                    sb.append(" UNION SELECT ");
                } else {
                    sb.append(" SELECT ");
                }
                
                for (TiDBColumn name : vs.keySet()) {
                    visit(vs.get(name).get(i));
                    sb.append(", ");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(sb.length() - 1);
            }
        } else {
            visit(allOperation.getRightExpr());
        }
        sb.append(")");
    }

    @Override
    public void visit(TiDBAnyOperator anyOperation) {
        sb.append("(");
        visit(anyOperation.getLeftExpr());
        sb.append(") ");
        sb.append(anyOperation.getOperator());
        sb.append(" ANY (");
        if (anyOperation.getRightExpr() instanceof TiDBValues) {
            TiDBValues values = (TiDBValues) anyOperation.getRightExpr();
            LinkedHashMap<TiDBColumn, List<TiDBConstant>> vs = values.getValues();
            int size = vs.get(vs.keySet().iterator().next()).size();
            for (int i = 0; i < size; i++) {
                if (i != 0) {
                    sb.append(" UNION SELECT ");
                } else {
                    sb.append(" SELECT ");
                }
                
                for (TiDBColumn name : vs.keySet()) {
                    visit(vs.get(name).get(i));
                    sb.append(", ");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(sb.length() - 1);
            }
        } else {
            visit(anyOperation.getRightExpr());
        }
        sb.append(")");
    }
}
