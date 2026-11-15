package sqlancer.voltdb.ast;

import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.ast.VoltDBBinaryOperation.VoltDBBinaryOperator;

/**
 * A subquery over one table, read under an alias so that it can be the outer query's table too:
 * <ul>
 * <li>{@code EXISTS (SELECT 1 FROM t AS s [WHERE q])}</li>
 * <li>{@code (x [NOT] IN (SELECT e FROM t AS s [WHERE q]))}</li>
 * <li>{@code (x op ANY|ALL (SELECT e FROM t AS s [WHERE q]))}</li>
 * <li>{@code (SELECT agg FROM t AS s [WHERE q])}, a scalar subquery, whose output is an aggregate so that it has one
 * row</li>
 * </ul>
 * The WHERE condition and the output may reference the outer query's columns (a correlated subquery). VoltDB runs a
 * subquery only over replicated tables ("Subquery expressions are only supported for single partition procedures and
 * AdHoc queries referencing only replicated tables"); the outer query may read partitioned tables.
 */
public final class VoltDBSubquery implements VoltDBExpression {

    public enum Form {
        EXISTS, IN, QUANTIFIED, SCALAR
    }

    private final Form form;
    private final VoltDBExpression left;
    private final boolean negated;
    private final VoltDBBinaryOperator operator;
    private final boolean all;
    private final String tableName;
    private final VoltDBTable alias;
    private final VoltDBExpression output;
    private final VoltDBExpression where;

    private VoltDBSubquery(Form form, VoltDBExpression left, boolean negated, VoltDBBinaryOperator operator,
            boolean all, String tableName, VoltDBTable alias, VoltDBExpression output, VoltDBExpression where) {
        this.form = form;
        this.left = left;
        this.negated = negated;
        this.operator = operator;
        this.all = all;
        this.tableName = tableName;
        this.alias = alias;
        this.output = output;
        this.where = where;
    }

    // alias is a copy of the table named by the alias, whose columns the output and WHERE condition reference
    public static VoltDBSubquery exists(String tableName, VoltDBTable alias, VoltDBExpression where) {
        return new VoltDBSubquery(Form.EXISTS, null, false, null, false, tableName, alias, null, where);
    }

    public static VoltDBSubquery inSet(VoltDBExpression left, boolean negated, String tableName, VoltDBTable alias,
            VoltDBExpression output, VoltDBExpression where) {
        return new VoltDBSubquery(Form.IN, left, negated, null, false, tableName, alias, output, where);
    }

    public static VoltDBSubquery quantified(VoltDBExpression left, VoltDBBinaryOperator operator, boolean all,
            String tableName, VoltDBTable alias, VoltDBExpression output, VoltDBExpression where) {
        return new VoltDBSubquery(Form.QUANTIFIED, left, false, operator, all, tableName, alias, output, where);
    }

    public static VoltDBSubquery scalar(String tableName, VoltDBTable alias, VoltDBAggregate output,
            VoltDBExpression where) {
        return new VoltDBSubquery(Form.SCALAR, null, false, null, false, tableName, alias, output, where);
    }

    public Form getForm() {
        return form;
    }

    // the left operand of IN or a quantified comparison
    public VoltDBExpression getLeft() {
        return left;
    }

    // NOT IN
    public boolean isNegated() {
        return negated;
    }

    // the comparison of a quantified comparison
    public VoltDBBinaryOperator getOperator() {
        return operator;
    }

    // ALL rather than ANY
    public boolean isAll() {
        return all;
    }

    public String getTableName() {
        return tableName;
    }

    public VoltDBTable getAlias() {
        return alias;
    }

    // the output column; null for EXISTS
    public VoltDBExpression getOutput() {
        return output;
    }

    // the WHERE condition, or null
    public VoltDBExpression getWhere() {
        return where;
    }

}
