package sqlancer.risingwave;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.risingwave.ast.RisingWaveAlias;
import sqlancer.risingwave.ast.RisingWaveArray;
import sqlancer.risingwave.ast.RisingWaveArrayAccess;
import sqlancer.risingwave.ast.RisingWaveCase;
import sqlancer.risingwave.ast.RisingWaveCast;
import sqlancer.risingwave.ast.RisingWaveColumnReference;
import sqlancer.risingwave.ast.RisingWaveConstant;
import sqlancer.risingwave.ast.RisingWaveCteReference;
import sqlancer.risingwave.ast.RisingWaveDerivedTable;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveFunctionCall;
import sqlancer.risingwave.ast.RisingWaveJoin;
import sqlancer.risingwave.ast.RisingWaveOperation;
import sqlancer.risingwave.ast.RisingWaveSelect;
import sqlancer.risingwave.ast.RisingWaveSubquery;
import sqlancer.risingwave.ast.RisingWaveTableReference;
import sqlancer.risingwave.ast.RisingWaveWindow;
import sqlancer.risingwave.ast.RisingWaveWith;

public final class RisingWaveToStringVisitor {
    private RisingWaveToStringVisitor() {
    }

    public static String identifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String list(List<? extends RisingWaveExpression> expressions) {
        return expressions.stream().map(RisingWaveToStringVisitor::asString).collect(Collectors.joining(", "));
    }

    private static String asString(RisingWaveOperation operation) {
        String operator = operation.operator.getText();
        List<RisingWaveExpression> arguments = operation.arguments;
        String first = asString(arguments.get(0));
        switch (operation.operator) {
        case BETWEEN:
        case NOT_BETWEEN:
            return "(" + first + " " + operator + " " + asString(arguments.get(1)) + " AND "
                    + asString(arguments.get(2)) + ")";
        case IN:
        case NOT_IN:
            return "(" + first + " " + operator + " (" + list(arguments.subList(1, arguments.size())) + "))";
        case NOT:
            return "(" + operator + " " + first + ")";
        case IS_NULL:
            return "(" + first + " " + operator + ")";
        default:
            return "(" + first + " " + operator + " " + asString(arguments.get(1)) + ")";
        }
    }

    public static String asString(RisingWaveExpression expression) {
        if (expression instanceof sqlancer.risingwave.ast.RisingWaveRow) {
            sqlancer.risingwave.ast.RisingWaveRow row = (sqlancer.risingwave.ast.RisingWaveRow) expression;
            return "CAST(ROW(" + list(row.fields) + ") AS " + row.type.sql() + ")";
        }
        if (expression instanceof sqlancer.risingwave.ast.RisingWaveFieldAccess) {
            sqlancer.risingwave.ast.RisingWaveFieldAccess field = (sqlancer.risingwave.ast.RisingWaveFieldAccess) expression;
            return "(" + asString(field.owner) + ")."
                    + identifier(field.owner.getType().fields().get(field.field).name);
        }
        if (expression instanceof RisingWaveArray) {
            RisingWaveArray array = (RisingWaveArray) expression;
            return "CAST(ARRAY[" + list(array.elements) + "] AS " + array.type.sql() + ")";
        } else if (expression instanceof RisingWaveArrayAccess) {
            RisingWaveArrayAccess access = (RisingWaveArrayAccess) expression;
            return "(" + asString(access.array) + ")[" + asString(access.index) + "]";
        } else if (expression instanceof RisingWaveConstant) {
            RisingWaveConstant literal = (RisingWaveConstant) expression;
            String value = literal.value == null ? "NULL" : "'" + literal.value.replace("'", "''") + "'";
            return "CAST(" + value + " AS " + literal.type.sql() + ")";
        } else if (expression instanceof RisingWaveColumnReference) {
            RisingWaveColumnReference column = (RisingWaveColumnReference) expression;
            return identifier(column.qualifier) + "." + identifier(column.name);
        } else if (expression instanceof RisingWaveOperation) {
            return asString((RisingWaveOperation) expression);
        } else if (expression instanceof RisingWaveFunctionCall) {
            RisingWaveFunctionCall call = (RisingWaveFunctionCall) expression;
            return call.getName() + "(" + (call.distinct ? "DISTINCT " : "")
                    + (call.arguments.isEmpty() ? "*" : list(call.arguments))
                    + (call.orderBy.isEmpty() ? ""
                            : " ORDER BY " + call.orderBy.stream()
                                    .map(o -> asString(o.expression) + " " + o.direction + " NULLS " + o.nullOrder)
                                    .collect(Collectors.joining(", ")))
                    + ")" + (call.filter == null ? "" : " FILTER (WHERE " + asString(call.filter) + ")");
        } else if (expression instanceof RisingWaveCast) {
            RisingWaveCast cast = (RisingWaveCast) expression;
            return "CAST(" + asString(cast.expression) + " AS " + cast.getType().sql() + ")";
        } else if (expression instanceof RisingWaveCase) {
            RisingWaveCase c = (RisingWaveCase) expression;
            return "(CASE WHEN " + asString(c.condition) + " THEN " + asString(c.thenExpression) + " ELSE "
                    + asString(c.elseExpression) + " END)";
        } else if (expression instanceof RisingWaveAlias) {
            RisingWaveAlias alias = (RisingWaveAlias) expression;
            return asString(alias.expression) + " AS " + identifier(alias.name);
        } else if (expression instanceof RisingWaveSubquery) {
            RisingWaveSubquery sub = (RisingWaveSubquery) expression;
            String query = "(" + sub.query.asString() + ")";
            switch (sub.kind) {
            case EXISTS:
                return "EXISTS " + query;
            case IN:
                return "(" + asString(sub.operand) + " IN " + query + ")";
            case SCALAR:
                return query;
            default:
                throw new AssertionError(sub.kind);
            }
        } else if (expression instanceof RisingWaveTableReference) {
            RisingWaveTableReference table = (RisingWaveTableReference) expression;
            return identifier(table.name) + " AS " + identifier(table.alias);
        } else if (expression instanceof RisingWaveDerivedTable) {
            RisingWaveDerivedTable derived = (RisingWaveDerivedTable) expression;
            String query = derived.query.asString();
            return "(" + query + ") AS " + identifier(derived.alias);
        } else if (expression instanceof RisingWaveCteReference) {
            RisingWaveCteReference reference = (RisingWaveCteReference) expression;
            return identifier(reference.name) + " AS " + identifier(reference.alias);
        } else if (expression instanceof RisingWaveWith) {
            RisingWaveWith with = (RisingWaveWith) expression;
            String definitions = with.definitions.stream()
                    .map(d -> identifier(d.name) + " AS (" + d.query.asString() + ")")
                    .collect(Collectors.joining(", "));
            return "(WITH " + definitions + " " + with.consumer.asString() + ") AS " + identifier(with.alias);
        } else if (expression instanceof RisingWaveWindow) {
            RisingWaveWindow window = (RisingWaveWindow) expression;
            return (window.hop ? "HOP" : "TUMBLE") + "(" + identifier(window.table.name) + ", "
                    + identifier(window.timestampColumn) + ", INTERVAL '" + window.slideSeconds + " seconds'"
                    + (window.hop ? ", INTERVAL '" + window.sizeSeconds + " seconds'" : "") + ") AS "
                    + identifier(window.alias);
        } else if (expression instanceof RisingWaveJoin) {
            RisingWaveJoin join = (RisingWaveJoin) expression;
            return "(" + asString(join.left) + " " + join.kind + " JOIN " + asString(join.right) + " ON "
                    + asString(join.condition) + ")";
        } else if (expression instanceof RisingWaveSelect) {
            RisingWaveSelect select = (RisingWaveSelect) expression;
            StringBuilder sql = new StringBuilder("SELECT ").append(list(select.getFetchColumns()));
            if (!select.getFromList().isEmpty()) {
                sql.append(" FROM ").append(list(select.getFromList()));
            }
            for (RisingWaveJoin join : select.getJoinClauses()) {
                sql.append(select.getFromList().isEmpty() ? " FROM " : ", ").append(asString(join));
            }
            clause(sql, " WHERE ", select.getWhereClause());
            if (!select.getGroupingSets().isEmpty()) {
                sql.append(" GROUP BY GROUPING SETS (").append(select.getGroupingSets().stream()
                        .map(set -> "(" + list(set) + ")").collect(Collectors.joining(", "))).append(")");
            } else if (!select.getGroupByClause().isEmpty()) {
                sql.append(" GROUP BY ").append(list(select.getGroupByClause()));
            }
            clause(sql, " HAVING ", select.getHavingClause());
            if (!select.getOrderByClauses().isEmpty()) {
                sql.append(" ORDER BY ").append(list(select.getOrderByClauses()));
            }
            clause(sql, " LIMIT ", select.getLimitClause());
            clause(sql, " OFFSET ", select.getOffsetClause());
            return sql.toString();
        }
        throw new AssertionError(expression.getClass());
    }

    private static void clause(StringBuilder sql, String keyword, RisingWaveExpression expression) {
        if (expression != null) {
            sql.append(keyword).append(asString(expression));
        }
    }
}
