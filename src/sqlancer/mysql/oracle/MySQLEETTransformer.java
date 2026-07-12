package sqlancer.mysql.oracle;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.oracle.EETTransformer;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCaseOperator;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation.UnaryPostfixOperator;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

/**
 * MySQL implementation of the {@link EETTransformer EET} tree-walker. Implements {@link #descend} to rebuild MySQL AST
 * nodes from their transformed children, threading the correct boolean/scalar context into each child.
 */
public class MySQLEETTransformer extends EETTransformer<MySQLExpression> {

    private static final boolean BOOLEAN = true;
    private static final boolean SCALAR = false;

    private final MySQLExpressionGenerator gen;

    public MySQLEETTransformer(MySQLExpressionGenerator gen) {
        this.gen = gen;
    }

    @Override
    protected MySQLExpression descend(MySQLExpression expr, boolean booleanContext) {
        if (expr instanceof MySQLBinaryLogicalOperation) {
            // AND/OR/XOR: both operands are evaluated in a boolean context.
            MySQLBinaryLogicalOperation op = (MySQLBinaryLogicalOperation) expr;
            return new MySQLBinaryLogicalOperation(transformNode(op.getLeft(), BOOLEAN, false),
                    transformNode(op.getRight(), BOOLEAN, false), op.getOp());
        } else if (expr instanceof MySQLBinaryComparisonOperation) {
            MySQLBinaryComparisonOperation op = (MySQLBinaryComparisonOperation) expr;
            return new MySQLBinaryComparisonOperation(transformNode(op.getLeft(), SCALAR, false),
                    transformNode(op.getRight(), SCALAR, false), op.getOp());
        } else if (expr instanceof MySQLBinaryOperation) {
            MySQLBinaryOperation op = (MySQLBinaryOperation) expr;
            return new MySQLBinaryOperation(transformNode(op.getLeft(), SCALAR, false),
                    transformNode(op.getRight(), SCALAR, false), op.getOp());
        } else if (expr instanceof MySQLUnaryPrefixOperation) {
            MySQLUnaryPrefixOperation op = (MySQLUnaryPrefixOperation) expr;
            boolean childContext = op.getOp() == MySQLUnaryPrefixOperator.NOT ? BOOLEAN : SCALAR;
            return new MySQLUnaryPrefixOperation(transformNode(op.getExpression(), childContext, false), op.getOp());
        } else if (expr instanceof MySQLUnaryPostfixOperation) {
            // The operand is transformed value-preservingly (scalar), which is safe for IS NULL/IS TRUE/IS FALSE.
            MySQLUnaryPostfixOperation op = (MySQLUnaryPostfixOperation) expr;
            return new MySQLUnaryPostfixOperation(transformNode(op.getExpression(), SCALAR, false), op.getOperator(),
                    op.isNegated());
        } else if (expr instanceof MySQLCastOperation) {
            MySQLCastOperation op = (MySQLCastOperation) expr;
            return new MySQLCastOperation(transformNode(op.getExpr(), SCALAR, false), op.getType());
        } else if (expr instanceof MySQLBetweenOperation) {
            MySQLBetweenOperation op = (MySQLBetweenOperation) expr;
            return new MySQLBetweenOperation(transformNode(op.getExpr(), SCALAR, false),
                    transformNode(op.getLeft(), SCALAR, false), transformNode(op.getRight(), SCALAR, false));
        } else if (expr instanceof MySQLInOperation) {
            MySQLInOperation op = (MySQLInOperation) expr;
            List<MySQLExpression> listElements = op.getListElements().stream()
                    .map(e -> transformNode(e, SCALAR, false)).collect(Collectors.toList());
            return new MySQLInOperation(transformNode(op.getExpr(), SCALAR, false), listElements, op.isTrue());
        } else if (expr instanceof MySQLComputableFunction) {
            MySQLComputableFunction op = (MySQLComputableFunction) expr;
            MySQLExpression[] args = op.getArguments();
            MySQLExpression[] newArgs = new MySQLExpression[args.length];
            for (int i = 0; i < args.length; i++) {
                newArgs[i] = transformNode(args[i], SCALAR, false);
            }
            return new MySQLComputableFunction(op.getFunction(), newArgs);
        } else if (expr instanceof MySQLCaseOperator) {
            return descendCase((MySQLCaseOperator) expr);
        }
        return expr;
    }

    private MySQLExpression descendCase(MySQLCaseOperator caseOp) {
        MySQLExpression switchCondition = caseOp.getSwitchCondition();
        // Without a switch operand the WHEN conditions are boolean; with one they are compared against the operand.
        boolean conditionContext = switchCondition == null ? BOOLEAN : SCALAR;
        MySQLExpression newSwitch = switchCondition == null ? null : transformNode(switchCondition, SCALAR, false);
        List<MySQLExpression> conditions = caseOp.getConditions().stream()
                .map(e -> transformNode(e, conditionContext, false)).collect(Collectors.toList());
        List<MySQLExpression> expressions = caseOp.getExpressions().stream().map(e -> transformNode(e, SCALAR, false))
                .collect(Collectors.toList());
        MySQLExpression elseExpr = caseOp.getElseExpr() == null ? null
                : transformNode(caseOp.getElseExpr(), SCALAR, false);
        return new MySQLCaseOperator(newSwitch, conditions, expressions, elseExpr);
    }

    @Override
    protected MySQLExpression and(MySQLExpression left, MySQLExpression right) {
        return new MySQLBinaryLogicalOperation(left, right, MySQLBinaryLogicalOperator.AND);
    }

    @Override
    protected MySQLExpression or(MySQLExpression left, MySQLExpression right) {
        return new MySQLBinaryLogicalOperation(left, right, MySQLBinaryLogicalOperator.OR);
    }

    @Override
    protected MySQLExpression not(MySQLExpression expr) {
        return new MySQLUnaryPrefixOperation(expr, MySQLUnaryPrefixOperator.NOT);
    }

    @Override
    protected MySQLExpression isNull(MySQLExpression expr) {
        return new MySQLUnaryPostfixOperation(expr, UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    protected MySQLExpression isNotNull(MySQLExpression expr) {
        return new MySQLUnaryPostfixOperation(expr, UnaryPostfixOperator.IS_NULL, true);
    }

    @Override
    protected MySQLExpression caseWhen(MySQLExpression condition, MySQLExpression thenExpr, MySQLExpression elseExpr) {
        return new MySQLCaseOperator(null, List.of(condition), List.of(thenExpr), elseExpr);
    }

    @Override
    protected MySQLExpression generateBooleanExpression() {
        return gen.generateBooleanExpression();
    }

    @Override
    protected boolean isCaseWhenApplicable(MySQLExpression expr) {
        // Table references cannot be wrapped in CASE WHEN (they would cause syntax errors, see rule No. 7 of the EET
        // paper); aggregates are excluded to avoid placing them in invalid contexts.
        return !(expr instanceof MySQLTableReference) && !(expr instanceof MySQLAggregate);
    }
}
