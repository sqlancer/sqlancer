package sqlancer.mysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.oracle.EETTransformation;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCaseOperator;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

/**
 * Recursively applies the {@link EETTransformation EET} transformation rules throughout a MySQL expression's AST. At
 * each node the transformer first recurses into (and rebuilds the node from) its transformed children, then, with some
 * probability, wraps the resulting sub-expression with a randomly chosen transformation rule.
 *
 * <p>
 * A boolean/scalar context flag is threaded through the recursion so that the determined-boolean rules (which reduce an
 * expression to a boolean value) are only ever applied where the expression is used purely for its truth value.
 */
public class MySQLEETTransformer {

    private static final boolean BOOLEAN = true;
    private static final boolean SCALAR = false;

    private final EETTransformation<MySQLExpression> transformation;

    public MySQLEETTransformer(MySQLExpressionGenerator gen) {
        this.transformation = new EETTransformation<>(new MySQLEETNodeFactory(gen));
    }

    /**
     * Transforms {@code expr} into a semantically equivalent expression. A transformation rule is always applied at the
     * root, guaranteeing that the returned expression differs from the input.
     */
    public MySQLExpression transform(MySQLExpression expr, boolean booleanContext) {
        return transformNode(expr, booleanContext, true);
    }

    private MySQLExpression transformNode(MySQLExpression expr, boolean booleanContext, boolean forceApply) {
        MySQLExpression descended = descend(expr, booleanContext);
        if (forceApply || Randomly.getBoolean()) {
            return transformation.applyRandomRule(descended, booleanContext);
        }
        return descended;
    }

    /**
     * Rebuilds {@code expr} with its children transformed. Leaf nodes (columns, constants, ...) and node types that are
     * not rebuilt here are returned unchanged; any applicable transformation is still applied to them by the calling
     * {@link #transformNode}.
     */
    private MySQLExpression descend(MySQLExpression expr, boolean booleanContext) {
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
}
