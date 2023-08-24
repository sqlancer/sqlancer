package sqlancer.transformations;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/**
 * This transformation simplifies complicated expressions e.g: a + (b + c) -> b.
 */

public class SimplifyExpressions extends JSQLParserBasedTransformation {
    public SimplifyExpressions() {
        super("simplify expressions. e.g. a + b -> a");
    }

    @Override
    public boolean init(String sql) {
        boolean baseSuc = super.init(sql);
        if (!baseSuc) {
            return false;
        }
        this.simplifier.setExpressionVisitor(expressionHandler);
        this.expressionHandler.setSelectVisitor(simplifier);
        return true;
    }

    private final ExpressionDeParser expressionHandler = new ExpressionDeParser() {
        @Override
        protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {

            Expression lhs = binaryExpression.getLeftExpression();
            Expression rhs = binaryExpression.getRightExpression();

            handleExpression(binaryExpression, lhs, BinaryExpression::setLeftExpression);
            handleExpression(binaryExpression, rhs, BinaryExpression::setRightExpression);

            super.visitBinaryExpression(binaryExpression, operator);
        }

    };
    private final SelectDeParser simplifier = new SelectDeParser() {

        @Override
        public void visit(PlainSelect plainSelect) {
            handleSelect(plainSelect);
            super.visit(plainSelect);
        }
    };

    @Override
    public void apply() {
        super.apply();
        if (statement instanceof Select) {
            Select select = (Select) statement;
            select.getSelectBody().accept(simplifier);
        }
    }

    private void handleSelect(PlainSelect plainSelect) {
        Expression where = plainSelect.getWhere();
        if (where != null) {
            handleExpression(plainSelect, where, PlainSelect::setWhere);
        }
        Expression having = plainSelect.getHaving();
        if (having != null) {
            handleExpression(plainSelect, having, PlainSelect::setHaving);
        }
    }

    private List<Expression> flattenExpression(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            return List.of(binaryExpression.getLeftExpression(), binaryExpression.getRightExpression());
        } else if (expression instanceof Parenthesis) {
            return List.of(((Parenthesis) expression).getExpression());
        }
        return new ArrayList<>();
    }

    private <P> void handleExpression(P parent, Expression expr, BiConsumer<P, Expression> setter) {

        List<Expression> expressions = flattenExpression(expr);
        for (Expression variant : expressions) {
            boolean suc = tryReplace(parent, expr, variant, setter);
            if (suc) {
                break;
            }
        }
    }
}
