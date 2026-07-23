package sqlancer.mysql.oracle;

import java.util.ArrayList;
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
import sqlancer.mysql.ast.MySQLCastOperation.CastType;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
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
 *
 * <p>
 * MySQL's expression generator is untyped, so type inference/generation works with a subset of MySQL's CAST target types
 * ({@link CastType}): {@link #inferType} conservatively classifies AST nodes into that domain (returning {@code null}
 * when uncertain), and {@link #generateExpressionOfType} pins the type of a random expression by wrapping it in a CAST.
 */
public class MySQLEETTransformer extends EETTransformer<MySQLExpression, MySQLCastOperation.CastType> {

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
            List<MySQLExpression> listElements = op.getListElements().stream().map(e -> transformNode(e, SCALAR, false))
                    .collect(Collectors.toList());
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
    protected MySQLExpression generateExpressionOfType(CastType type) {
        // The MySQL expression generator is untyped, so the type of an arbitrary random expression is pinned by
        // wrapping it in a CAST to the requested type.
        return new MySQLCastOperation(gen.generateExpression(), type);
    }

    @Override
    protected CastType inferType(MySQLExpression expr) {
        if (expr instanceof MySQLBinaryLogicalOperation || expr instanceof MySQLBinaryComparisonOperation
                || expr instanceof MySQLUnaryPostfixOperation || expr instanceof MySQLBetweenOperation
                || expr instanceof MySQLInOperation || expr instanceof MySQLExists) {
            // Predicates evaluate to the boolean values 0/1, which are signed BIGINT.
            return CastType.SIGNED;
        } else if (expr instanceof MySQLBinaryOperation) {
            // The bit operators &, | and ^ return BIGINT UNSIGNED.
            return CastType.UNSIGNED;
        } else if (expr instanceof MySQLCastOperation) {
            return ((MySQLCastOperation) expr).getType();
        } else if (expr instanceof MySQLUnaryPrefixOperation) {
            return inferUnaryPrefixType((MySQLUnaryPrefixOperation) expr);
        } else if (expr instanceof MySQLConstant) {
            return inferConstantType((MySQLConstant) expr);
        } else if (expr instanceof MySQLColumnReference) {
            return inferColumnType((MySQLColumnReference) expr);
        } else if (expr instanceof MySQLComputableFunction) {
            return inferFunctionType((MySQLComputableFunction) expr);
        } else if (expr instanceof MySQLCaseOperator) {
            return inferCaseType((MySQLCaseOperator) expr);
        }
        return null;
    }

    private CastType inferUnaryPrefixType(MySQLUnaryPrefixOperation op) {
        if (op.getOp() == MySQLUnaryPrefixOperator.NOT) {
            return CastType.SIGNED;
        }
        CastType operandType = inferType(op.getExpression());
        if (op.getOp() == MySQLUnaryPrefixOperator.PLUS) {
            return operandType;
        }
        if (op.getOp() == MySQLUnaryPrefixOperator.MINUS) {
            if (operandType == CastType.UNSIGNED) {
                return CastType.SIGNED;
            } else if (operandType == CastType.FLOAT) {
                return CastType.DOUBLE;
            } else if (operandType != CastType.CHAR) {
                return operandType;
            }
        }
        return null;
    }

    private CastType inferConstantType(MySQLConstant constant) {
        if (constant instanceof MySQLConstant.MySQLIntConstant) {
            return constant.isSigned() ? CastType.SIGNED : CastType.UNSIGNED;
        } else if (constant instanceof MySQLConstant.MySQLTextConstant) {
            return CastType.CHAR;
        } else if (constant instanceof MySQLConstant.MySQLDoubleConstant) {
            return CastType.DOUBLE;
        }
        return null; // the NULL constant has no type of its own
    }

    private CastType inferColumnType(MySQLColumnReference ref) {
        switch (ref.getColumn().getType()) {
        case INT:
            return CastType.SIGNED; // the table generator never creates UNSIGNED INT columns
        case VARCHAR:
            return CastType.CHAR;
        case FLOAT:
            // Assumes FLOAT columns are never created with (M, D); otherwise the CAST would need the exact
            // precision/scale.
            return CastType.FLOAT;
        case DOUBLE:
            // Assumes DOUBLE columns are never created with (M, D); otherwise the CAST would need the exact
            // precision/scale.
            return CastType.DOUBLE;
        case DECIMAL:
            // Assumes DECIMAL columns are never created with (M, D); otherwise the CAST would need the exact
            // precision/scale.
            return CastType.DECIMAL;
        default:
            return null;
        }
    }

    private CastType inferFunctionType(MySQLComputableFunction func) {
        MySQLExpression[] args = func.getArguments();
        switch (func.getFunction()) {
        case BIT_COUNT:
            return CastType.SIGNED;
        case IF:
            // The result type aggregates the types of the two value arguments (the condition does not contribute).
            return commonType(args[1], args[2]);
        case COALESCE:
        case IFNULL:
        case LEAST:
        case GREATEST:
            return commonType(args);
        default:
            return null;
        }
    }

    private CastType inferCaseType(MySQLCaseOperator caseOp) {
        List<MySQLExpression> branches = new ArrayList<>(caseOp.getExpressions());
        if (caseOp.getElseExpr() != null) {
            branches.add(caseOp.getElseExpr());
        }
        return commonType(branches.toArray(new MySQLExpression[0]));
    }

    /**
     * The common type of several result-type-determining subexpressions, or {@code null} if they do not have the same
     * inferrable type (a conservative under-approximation of MySQL's aggregation rules).
     */
    private CastType commonType(MySQLExpression... exprs) {
        CastType common = null;
        for (MySQLExpression expr : exprs) {
            CastType type = inferType(expr);
            if (type == null || common != null && type != common) {
                return null;
            }
            common = type;
        }
        return common;
    }

    @Override
    protected boolean isCaseWhenApplicable(MySQLExpression expr) {
        // Table references cannot be wrapped in CASE WHEN (they would cause syntax errors, see rule No. 7 of the EET
        // paper); aggregates are excluded to avoid placing them in invalid contexts.
        return !(expr instanceof MySQLTableReference) && !(expr instanceof MySQLAggregate);
    }
}
