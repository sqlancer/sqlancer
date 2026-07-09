package sqlancer.mysql.gen;

import java.util.List;

import sqlancer.common.oracle.EETNodeFactory;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import sqlancer.mysql.ast.MySQLCaseOperator;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation.UnaryPostfixOperator;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

/**
 * Constructs the MySQL AST nodes needed by the {@link sqlancer.common.oracle.EETTransformation EET transformation}
 * rules.
 */
public class MySQLEETNodeFactory implements EETNodeFactory<MySQLExpression> {

    private final MySQLExpressionGenerator gen;

    public MySQLEETNodeFactory(MySQLExpressionGenerator gen) {
        this.gen = gen;
    }

    @Override
    public MySQLExpression and(MySQLExpression left, MySQLExpression right) {
        return new MySQLBinaryLogicalOperation(left, right, MySQLBinaryLogicalOperator.AND);
    }

    @Override
    public MySQLExpression or(MySQLExpression left, MySQLExpression right) {
        return new MySQLBinaryLogicalOperation(left, right, MySQLBinaryLogicalOperator.OR);
    }

    @Override
    public MySQLExpression not(MySQLExpression expr) {
        return new MySQLUnaryPrefixOperation(expr, MySQLUnaryPrefixOperator.NOT);
    }

    @Override
    public MySQLExpression isNull(MySQLExpression expr) {
        return new MySQLUnaryPostfixOperation(expr, UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public MySQLExpression isNotNull(MySQLExpression expr) {
        return new MySQLUnaryPostfixOperation(expr, UnaryPostfixOperator.IS_NULL, true);
    }

    @Override
    public MySQLExpression caseWhen(MySQLExpression condition, MySQLExpression thenExpr, MySQLExpression elseExpr) {
        return new MySQLCaseOperator(null, List.of(condition), List.of(thenExpr), elseExpr);
    }

    @Override
    public MySQLExpression generateBooleanExpression() {
        return gen.generateBooleanExpression();
    }

    @Override
    public boolean isCaseWhenApplicable(MySQLExpression expr) {
        // Table references cannot be wrapped in CASE WHEN (they would cause syntax errors, see rule No. 7 of the EET
        // paper); aggregates are excluded to avoid placing them in invalid contexts.
        return !(expr instanceof MySQLTableReference) && !(expr instanceof MySQLAggregate);
    }
}
