package sqlancer.yugabyte.ysql;

import java.util.List;
import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLAggregate;
import sqlancer.yugabyte.ysql.ast.YSQLBetweenOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryLogicalOperation;
import sqlancer.yugabyte.ysql.ast.YSQLCastOperation;
import sqlancer.yugabyte.ysql.ast.YSQLJSONBOperation;
import sqlancer.yugabyte.ysql.ast.YSQLJSONBFunction;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLFunction;
import sqlancer.yugabyte.ysql.ast.YSQLInOperation;
import sqlancer.yugabyte.ysql.ast.YSQLJoin;
import sqlancer.yugabyte.ysql.ast.YSQLJoin.YSQLJoinType;
import sqlancer.yugabyte.ysql.ast.YSQLOrderByTerm;
import sqlancer.yugabyte.ysql.ast.YSQLPOSIXRegularExpression;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixText;
import sqlancer.yugabyte.ysql.ast.YSQLPrefixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.YSQLFromTable;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.YSQLSubquery;
import sqlancer.yugabyte.ysql.ast.YSQLSimilarTo;

public final class YSQLToStringVisitor extends ToStringVisitor<YSQLExpression> implements YSQLVisitor {

    @Override
    public void visitSpecific(YSQLExpression expr) {
        YSQLVisitor.super.visit(expr);
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(YSQLConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public void visit(YSQLPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorTextRepresentation());
    }

    @Override
    public void visit(YSQLColumnValue c) {
        sb.append(c.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(YSQLPrefixOperation op) {
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(YSQLSelect s) {
        sb.append("SELECT ");
        switch (s.getSelectOption()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            if (s.getDistinctOnClause() != null) {
                sb.append("ON (");
                visit(s.getDistinctOnClause());
                sb.append(") ");
            }
            break;
        case ALL:
            sb.append(Randomly.fromOptions("ALL ", ""));
            break;
        default:
            throw new AssertionError();
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(s.getFetchColumns());
        }
        sb.append(" FROM ");
        visit(s.getFromList());

        for (YSQLJoin j : s.getJoinClauses()) {
            sb.append(" ");
            switch (j.getType()) {
            case INNER:
                if (Randomly.getBoolean()) {
                    sb.append("INNER ");
                }
                sb.append("JOIN");
                break;
            case LEFT:
                sb.append("LEFT OUTER JOIN");
                break;
            case RIGHT:
                sb.append("RIGHT OUTER JOIN");
                break;
            case FULL:
                sb.append("FULL OUTER JOIN");
                break;
            case CROSS:
                sb.append("CROSS JOIN");
                break;
            default:
                throw new AssertionError(j.getType());
            }
            sb.append(" ");
            visit(j.getTableReference());
            if (j.getType() != YSQLJoinType.CROSS) {
                sb.append(" ON ");
                visit(j.getOnClause());
            }
        }

        if (s.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(s.getWhereClause());
        }
        if (s.getGroupByExpressions().size() > 0) {
            sb.append(" GROUP BY ");
            visit(s.getGroupByExpressions());
        }
        if (s.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(s.getHavingClause());

        }
        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(s.getOrderByExpressions());
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
    public void visit(YSQLOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder());
    }

    @Override
    public void visit(YSQLFunction f) {
        sb.append(f.getFunctionName());
        sb.append("(");
        int i = 0;
        for (YSQLExpression arg : f.getArguments()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(arg);
        }
        sb.append(")");
    }

    @Override
    public void visit(YSQLCastOperation cast) {
        if (Randomly.getBoolean()) {
            sb.append("CAST(");
            visit(cast.getExpression());
            sb.append(" AS ");
            appendType(cast);
            sb.append(")");
        } else {
            sb.append("(");
            visit(cast.getExpression());
            sb.append(")::");
            appendType(cast);
        }
    }

    @Override
    public void visit(YSQLBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN ");
        if (op.isSymmetric()) {
            sb.append("SYMMETRIC ");
        }
        sb.append("(");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(YSQLInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        visit(op.getListElements());
        sb.append(")");
    }

    @Override
    public void visit(YSQLPostfixText op) {
        visit(op.getExpr());
        sb.append(op.getText());
    }

    @Override
    public void visit(YSQLAggregate op) {
        sb.append(op.getFunction());
        sb.append("(");
        visit(op.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(YSQLSimilarTo op) {
        sb.append("(");
        visit(op.getString());
        sb.append(" SIMILAR TO ");
        visit(op.getSimilarTo());
        if (op.getEscapeCharacter() != null) {
            visit(op.getEscapeCharacter());
        }
        sb.append(")");
    }

    @Override
    public void visit(YSQLPOSIXRegularExpression op) {
        visit(op.getString());
        sb.append(op.getOp().getStringRepresentation());
        visit(op.getRegex());
    }

    @Override
    public void visit(YSQLFromTable from) {
        if (from.isOnly()) {
            sb.append("ONLY ");
        }
        sb.append(from.getTable().getName());
        if (!from.isOnly() && Randomly.getBoolean()) {
            sb.append("*");
        }
    }

    @Override
    public void visit(YSQLSubquery subquery) {
        sb.append("(");
        visit(subquery.getSelect());
        sb.append(") AS ");
        sb.append(subquery.getName());
    }

    @Override
    public void visit(YSQLBinaryLogicalOperation op) {
        super.visit((BinaryOperation<YSQLExpression>) op);
    }
    
    @Override
    public void visit(YSQLJSONBOperation op) {
        super.visit((BinaryOperation<YSQLExpression>) op);
    }
    
    @Override
    public void visit(YSQLJSONBFunction func) {
        sb.append(func.getFunctionName());
        sb.append("(");
        List<YSQLExpression> args = func.getArguments();
        for (int i = 0; i < args.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(args.get(i));
        }
        sb.append(")");
    }

    private void appendType(YSQLCastOperation cast) {
        YSQLCompoundDataType compoundType = cast.getCompoundType();
        switch (compoundType.getDataType()) {
        case BOOLEAN:
            sb.append("BOOLEAN");
            break;
        case SMALLINT:
            sb.append("SMALLINT");
            break;
        case INT:
            sb.append("INT");
            break;
        case BIGINT:
            sb.append("BIGINT");
            break;
        case NUMERIC:
            sb.append("NUMERIC");
            break;
        case DECIMAL:
            sb.append("DECIMAL");
            break;
        case REAL:
            sb.append("REAL");
            break;
        case DOUBLE_PRECISION:
            sb.append("DOUBLE PRECISION");
            break;
        case FLOAT:
            sb.append("FLOAT");
            break;
        case VARCHAR:
            sb.append("VARCHAR");
            break;
        case CHAR:
            sb.append("CHAR");
            break;
        case TEXT:
            sb.append("TEXT");
            break;
        case DATE:
            sb.append("DATE");
            break;
        case TIME:
            sb.append("TIME");
            break;
        case TIMESTAMP:
            sb.append("TIMESTAMP");
            break;
        case TIMESTAMPTZ:
            sb.append("TIMESTAMPTZ");
            break;
        case INTERVAL:
            sb.append("INTERVAL");
            break;
        case UUID:
            sb.append("UUID");
            break;
        case JSON:
            sb.append("JSON");
            break;
        case JSONB:
            sb.append("JSONB");
            break;
        case INT4RANGE:
            sb.append("int4range");
            break;
        case INT8RANGE:
            sb.append("int8range");
            break;
        case NUMRANGE:
            sb.append("numrange");
            break;
        case TSRANGE:
            sb.append("tsrange");
            break;
        case TSTZRANGE:
            sb.append("tstzrange");
            break;
        case DATERANGE:
            sb.append("daterange");
            break;
        case RANGE:
            sb.append("int4range");
            break;
        case MONEY:
            sb.append("MONEY");
            break;
        case INET:
            sb.append("INET");
            break;
        case CIDR:
            sb.append("CIDR");
            break;
        case MACADDR:
            sb.append("MACADDR");
            break;
        case POINT:
            sb.append("POINT");
            break;
        case LINE:
            sb.append("LINE");
            break;
        case LSEG:
            sb.append("LSEG");
            break;
        case BOX:
            sb.append("BOX");
            break;
        case PATH:
            sb.append("PATH");
            break;
        case POLYGON:
            sb.append("POLYGON");
            break;
        case CIRCLE:
            sb.append("CIRCLE");
            break;
        case BIT:
            sb.append("BIT");
            break;
        case BYTEA:
            sb.append("BYTEA");
            break;
        case INT_ARRAY:
            sb.append("INT[]");
            break;
        case TEXT_ARRAY:
            sb.append("TEXT[]");
            break;
        case BOOLEAN_ARRAY:
            sb.append("BOOLEAN[]");
            break;
        default:
            throw new AssertionError(cast.getType());
        }
        Optional<Integer> size = compoundType.getSize();
        if (size.isPresent()) {
            sb.append("(");
            sb.append(size.get());
            sb.append(")");
        }
    }

}
