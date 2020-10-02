package sqlancer.sqlite3.ast;

import java.util.List;
import java.util.Optional;

import sqlancer.IgnoreMeException;
import sqlancer.LikeImplementationHelper;
import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.sqlite3.SQLite3CollateHelper;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation.BinaryOperator;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public abstract class SQLite3Expression {

    public static class SQLite3TableReference extends SQLite3Expression {

        private final String indexedBy;
        private final SQLite3Table table;

        public SQLite3TableReference(String indexedBy, SQLite3Table table) {
            this.indexedBy = indexedBy;
            this.table = table;
        }

        public SQLite3TableReference(SQLite3Table table) {
            this.indexedBy = null;
            this.table = table;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

        public SQLite3Table getTable() {
            return table;
        }

        public String getIndexedBy() {
            return indexedBy;
        }

    }

    public static class SQLite3Distinct extends SQLite3Expression {

        private final SQLite3Expression expr;

        public SQLite3Distinct(SQLite3Expression expr) {
            this.expr = expr;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return expr.getExplicitCollateSequence();
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            return expr.getExpectedValue();
        }

        public SQLite3Expression getExpression() {
            return expr;
        }

        @Override
        public SQLite3CollateSequence getImplicitCollateSequence() {
            // https://www.sqlite.org/src/tktview/18ab5da2c05ad57d7f9d79c41d3138b141378543
            return expr.getImplicitCollateSequence();
        }

    }

    public SQLite3Constant getExpectedValue() {
        return null;
    }

    public enum TypeAffinity {
        INTEGER, TEXT, BLOB, REAL, NUMERIC, NONE;

        public boolean isNumeric() {
            return this == INTEGER || this == REAL || this == NUMERIC;
        }
    }

    /*
     * See https://www.sqlite.org/datatype3.html 3.2
     */
    public TypeAffinity getAffinity() {
        return TypeAffinity.NONE;
    }

    /*
     * See https://www.sqlite.org/datatype3.html#assigning_collating_sequences_from_sql 7.1
     *
     */
    public abstract SQLite3CollateSequence getExplicitCollateSequence();

    public SQLite3CollateSequence getImplicitCollateSequence() {
        return null;
    }

    public static class SQLite3Exist extends SQLite3Expression {

        private final SQLite3Expression select;

        public SQLite3Exist(SQLite3Expression select) {
            this.select = select;
        }

        public SQLite3Expression getExpression() {
            return select;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class Join extends SQLite3Expression {

        public enum JoinType {
            INNER, CROSS, OUTER, NATURAL;
        }

        private final SQLite3Table table;
        private SQLite3Expression onClause;
        private JoinType type;

        public Join(Join other) {
            this.table = other.table;
            this.onClause = other.onClause;
            this.type = other.type;
        }

        public Join(SQLite3Table table, SQLite3Expression onClause, JoinType type) {
            this.table = table;
            this.onClause = onClause;
            this.type = type;
        }

        public Join(SQLite3Table table, JoinType type) {
            this.table = table;
            if (type != JoinType.NATURAL) {
                throw new AssertionError();
            }
            this.onClause = null;
            this.type = type;
        }

        public SQLite3Table getTable() {
            return table;
        }

        public SQLite3Expression getOnClause() {
            return onClause;
        }

        public JoinType getType() {
            return type;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

        public void setOnClause(SQLite3Expression onClause) {
            this.onClause = onClause;
        }

        public void setType(JoinType type) {
            this.type = type;
        }

    }

    public static class Subquery extends SQLite3Expression {

        private final String query;

        public Subquery(String query) {
            this.query = query;
        }

        public static SQLite3Expression create(String query) {
            return new Subquery(query);
        }

        public String getQuery() {
            return query;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class TypeLiteral {

        private final Type type;

        public enum Type {
            TEXT {
                @Override
                public SQLite3Constant apply(SQLite3Constant cons) {
                    return SQLite3Cast.castToText(cons);
                }
            },
            REAL {
                @Override
                public SQLite3Constant apply(SQLite3Constant cons) {
                    return SQLite3Cast.castToReal(cons);
                }
            },
            INTEGER {
                @Override
                public SQLite3Constant apply(SQLite3Constant cons) {
                    return SQLite3Cast.castToInt(cons);
                }
            },
            NUMERIC {
                @Override
                public SQLite3Constant apply(SQLite3Constant cons) {
                    return SQLite3Cast.castToNumeric(cons);
                }
            },
            BLOB {
                @Override
                public SQLite3Constant apply(SQLite3Constant cons) {
                    return SQLite3Cast.castToBlob(cons);
                }
            };

            public abstract SQLite3Constant apply(SQLite3Constant cons);
        }

        public TypeLiteral(Type type) {
            this.type = type;
        }

        public Type getType() {
            return type;
        }

    }

    public static class Cast extends SQLite3Expression {

        private final TypeLiteral type;
        private final SQLite3Expression expression;

        public Cast(TypeLiteral typeofExpr, SQLite3Expression expression) {
            this.type = typeofExpr;
            this.expression = expression;
        }

        public SQLite3Expression getExpression() {
            return expression;
        }

        public TypeLiteral getType() {
            return type;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            if (expression.getExpectedValue() == null) {
                return null;
            } else {
                return type.type.apply(expression.getExpectedValue());
            }
        }

        /**
         * An expression of the form "CAST(expr AS type)" has an affinity that is the same as a column with a declared
         * type of "type".
         */
        @Override
        public TypeAffinity getAffinity() {
            switch (type.type) {
            case BLOB:
                return TypeAffinity.BLOB;
            case INTEGER:
                return TypeAffinity.INTEGER;
            case NUMERIC:
                return TypeAffinity.NUMERIC;
            case REAL:
                return TypeAffinity.REAL;
            case TEXT:
                return TypeAffinity.TEXT;
            default:
                throw new AssertionError();
            }
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return expression.getExplicitCollateSequence();
        }

        @Override
        public SQLite3CollateSequence getImplicitCollateSequence() {
            if (SQLite3CollateHelper.shouldGetSubexpressionAffinity(expression)) {
                return expression.getImplicitCollateSequence();
            } else {
                return null;
            }
        }

    }

    public static class BetweenOperation extends SQLite3Expression {

        private final SQLite3Expression expr;
        private final boolean negated;
        private final SQLite3Expression left;
        private final SQLite3Expression right;

        public BetweenOperation(SQLite3Expression expr, boolean negated, SQLite3Expression left,
                SQLite3Expression right) {
            this.expr = expr;
            this.negated = negated;
            this.left = left;
            this.right = right;
        }

        public SQLite3Expression getExpression() {
            return expr;
        }

        public boolean isNegated() {
            return negated;
        }

        public SQLite3Expression getLeft() {
            return left;
        }

        public SQLite3Expression getRight() {
            return right;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            if (expr.getExplicitCollateSequence() != null) {
                return expr.getExplicitCollateSequence();
            } else if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            return getTopNode().getExpectedValue();
        }

        public SQLite3Expression getTopNode() {
            BinaryComparisonOperation leftOp = new BinaryComparisonOperation(expr, left,
                    BinaryComparisonOperator.GREATER_EQUALS);
            BinaryComparisonOperation rightOp = new BinaryComparisonOperation(expr, right,
                    BinaryComparisonOperator.SMALLER_EQUALS);
            Sqlite3BinaryOperation and = new Sqlite3BinaryOperation(leftOp, rightOp, BinaryOperator.AND);
            if (negated) {
                return new SQLite3UnaryOperation(UnaryOperator.NOT, and);
            } else {
                return and;
            }
        }

    }

    public static class Function extends SQLite3Expression {

        private final SQLite3Expression[] arguments;
        private final String name;

        public Function(String name, SQLite3Expression... arguments) {
            this.name = name;
            this.arguments = arguments.clone();
        }

        public SQLite3Expression[] getArguments() {
            return arguments.clone();
        }

        public String getName() {
            return name;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            for (SQLite3Expression arg : arguments) {
                if (arg.getExplicitCollateSequence() != null) {
                    return arg.getExplicitCollateSequence();
                }
            }
            return null;
        }

    }

    public static class SQLite3OrderingTerm extends SQLite3Expression {

        private final SQLite3Expression expression;
        private final Ordering ordering;

        public enum Ordering {
            ASC, DESC;

            public static Ordering getRandomValue() {
                return Randomly.fromOptions(Ordering.values());
            }
        }

        public SQLite3OrderingTerm(SQLite3Expression expression, Ordering ordering) {
            this.expression = expression;
            this.ordering = ordering;
        }

        public SQLite3Expression getExpression() {
            return expression;
        }

        public Ordering getOrdering() {
            return ordering;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return expression.getExplicitCollateSequence();
        }

    }

    public static class CollateOperation extends SQLite3Expression {

        private final SQLite3Expression expression;
        private final SQLite3CollateSequence collate;

        public CollateOperation(SQLite3Expression expression, SQLite3CollateSequence collate) {
            this.expression = expression;
            this.collate = collate;
        }

        public SQLite3CollateSequence getCollate() {
            return collate;
        }

        public SQLite3Expression getExpression() {
            return expression;
        }

        // If either operand has an explicit collating function assignment using the
        // postfix COLLATE operator, then the explicit collating function is used for
        // comparison, with precedence to the collating function of the left operand.
        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return collate;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            return expression.getExpectedValue();
        }

        @Override
        public TypeAffinity getAffinity() {
            return expression.getAffinity();
        }

    }

    public static class SQLite3PostfixUnaryOperation extends SQLite3Expression
            implements UnaryOperation<SQLite3Expression> {

        public enum PostfixUnaryOperator {
            ISNULL("ISNULL") {
                @Override
                public SQLite3Constant apply(SQLite3Constant expectedValue) {
                    if (expectedValue.isNull()) {
                        return SQLite3Constant.createTrue();
                    } else {
                        return SQLite3Constant.createFalse();
                    }
                }
            },
            NOT_NULL("NOT NULL") {
                @Override
                public SQLite3Constant apply(SQLite3Constant expectedValue) {
                    if (expectedValue.isNull()) {
                        return SQLite3Constant.createFalse();
                    } else {
                        return SQLite3Constant.createTrue();
                    }
                }

            },

            NOTNULL("NOTNULL") {

                @Override
                public SQLite3Constant apply(SQLite3Constant expectedValue) {
                    if (expectedValue.isNull()) {
                        return SQLite3Constant.createFalse();
                    } else {
                        return SQLite3Constant.createTrue();
                    }
                }

            },
            IS_TRUE("IS TRUE") {

                @Override
                public SQLite3Constant apply(SQLite3Constant expectedValue) {
                    if (expectedValue.isNull()) {
                        return SQLite3Constant.createIntConstant(0);
                    }
                    return SQLite3Cast.asBoolean(expectedValue);
                }
            },
            IS_FALSE("IS FALSE") {

                @Override
                public SQLite3Constant apply(SQLite3Constant expectedValue) {
                    if (expectedValue.isNull()) {
                        return SQLite3Constant.createIntConstant(0);
                    }
                    return SQLite3UnaryOperation.UnaryOperator.NOT.apply(SQLite3Cast.asBoolean(expectedValue));
                }

            };

            private final String textRepresentation;

            PostfixUnaryOperator(String textRepresentation) {
                this.textRepresentation = textRepresentation;
            }

            @Override
            public String toString() {
                return getTextRepresentation();
            }

            public String getTextRepresentation() {
                return textRepresentation;
            }

            public static PostfixUnaryOperator getRandomOperator() {
                return Randomly.fromOptions(values());
            }

            public abstract SQLite3Constant apply(SQLite3Constant expectedValue);

        }

        private final PostfixUnaryOperator operation;
        private final SQLite3Expression expression;

        public SQLite3PostfixUnaryOperation(PostfixUnaryOperator operation, SQLite3Expression expression) {
            this.operation = operation;
            this.expression = expression;
        }

        public PostfixUnaryOperator getOperation() {
            return operation;
        }

        @Override
        public SQLite3Expression getExpression() {
            return expression;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            if (expression.getExpectedValue() == null) {
                return null;
            }
            return operation.apply(expression.getExpectedValue());
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return expression.getExplicitCollateSequence();
        }

        @Override
        public String getOperatorRepresentation() {
            return operation.getTextRepresentation();
        }

        @Override
        public OperatorKind getOperatorKind() {
            return OperatorKind.POSTFIX;
        }

    }

    public static class InOperation extends SQLite3Expression {

        private final SQLite3Expression left;
        private List<SQLite3Expression> rightExpressionList;
        private SQLite3Expression rightSelect;

        public InOperation(SQLite3Expression left, List<SQLite3Expression> right) {
            this.left = left;
            this.rightExpressionList = right;
        }

        public InOperation(SQLite3Expression left, SQLite3Expression select) {
            this.left = left;
            this.rightSelect = select;
        }

        public SQLite3Expression getLeft() {
            return left;
        }

        public List<SQLite3Expression> getRightExpressionList() {
            return rightExpressionList;
        }

        public SQLite3Expression getRightSelect() {
            return rightSelect;
        }

        @Override
        // The collating sequence used for expressions of the form "x IN (y, z, ...)" is
        // the collating sequence of x.
        public SQLite3CollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return null;
            }
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            // TODO query as right hand side is not implemented
            if (left.getExpectedValue() == null) {
                return null;
            }
            if (rightExpressionList.isEmpty()) {
                return SQLite3Constant.createFalse();
            } else if (left.getExpectedValue().isNull()) {
                return SQLite3Constant.createNullConstant();
            } else {
                boolean containsNull = false;
                for (SQLite3Expression expr : getRightExpressionList()) {
                    if (expr.getExpectedValue() == null) {
                        return null; // TODO: we can still compute something if the value is already contained
                    }
                    SQLite3CollateSequence collate = getExplicitCollateSequence();
                    if (collate == null) {
                        collate = left.getImplicitCollateSequence();
                    }
                    if (collate == null) {
                        collate = SQLite3CollateSequence.BINARY;
                    }
                    ConstantTuple convertedConstants = applyAffinities(left.getAffinity(), TypeAffinity.NONE,
                            left.getExpectedValue(), expr.getExpectedValue());
                    SQLite3Constant equals = left.getExpectedValue().applyEquals(convertedConstants.right, collate);
                    Optional<Boolean> isEquals = SQLite3Cast.isTrue(equals);
                    if (isEquals.isPresent() && isEquals.get()) {
                        return SQLite3Constant.createTrue();
                    } else if (!isEquals.isPresent()) {
                        containsNull = true;
                    }
                }
                if (containsNull) {
                    return SQLite3Constant.createNullConstant();
                } else {
                    return SQLite3Constant.createFalse();
                }
            }
        }
    }

    public static class MatchOperation extends SQLite3Expression {

        private final SQLite3Expression left;
        private final SQLite3Expression right;

        public MatchOperation(SQLite3Expression left, SQLite3Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

        public SQLite3Expression getLeft() {
            return left;
        }

        public SQLite3Expression getRight() {
            return right;
        }

    }

    public static class BinaryComparisonOperation extends SQLite3Expression
            implements BinaryOperation<SQLite3Expression> {

        private final BinaryComparisonOperator operation;
        private final SQLite3Expression left;
        private final SQLite3Expression right;

        public BinaryComparisonOperation(SQLite3Expression left, SQLite3Expression right,
                BinaryComparisonOperator operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;
        }

        public BinaryComparisonOperator getOperator() {
            return operation;
        }

        @Override
        public SQLite3Expression getLeft() {
            return left;
        }

        @Override
        public SQLite3Expression getRight() {
            return right;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            SQLite3Constant leftExpected = left.getExpectedValue();
            SQLite3Constant rightExpected = right.getExpectedValue();
            if (leftExpected == null || rightExpected == null) {
                return null;
            }
            TypeAffinity leftAffinity = left.getAffinity();
            TypeAffinity rightAffinity = right.getAffinity();
            return operation.applyOperand(leftExpected, leftAffinity, rightExpected, rightAffinity, left, right,
                    operation.shouldApplyAffinity());
        }

        public static BinaryComparisonOperation create(SQLite3Expression leftVal, SQLite3Expression rightVal,
                BinaryComparisonOperator op) {
            return new BinaryComparisonOperation(leftVal, rightVal, op);
        }

        public enum BinaryComparisonOperator {
            SMALLER("<") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    return left.applyLess(right, collate);
                }

            },
            SMALLER_EQUALS("<=") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    SQLite3Constant lessThan = left.applyLess(right, collate);
                    if (lessThan == null) {
                        return null;
                    }
                    if (lessThan.getDataType() == SQLite3DataType.INT && lessThan.asInt() == 0) {
                        return left.applyEquals(right, collate);
                    } else {
                        return lessThan;
                    }
                }

            },
            GREATER(">") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    SQLite3Constant equals = left.applyEquals(right, collate);
                    if (equals == null) {
                        return null;
                    }
                    if (equals.getDataType() == SQLite3DataType.INT && equals.asInt() == 1) {
                        return SQLite3Constant.createFalse();
                    } else {
                        SQLite3Constant applyLess = left.applyLess(right, collate);
                        if (applyLess == null) {
                            return null;
                        }
                        return UnaryOperator.NOT.apply(applyLess);
                    }
                }

            },
            GREATER_EQUALS(">=") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    SQLite3Constant equals = left.applyEquals(right, collate);
                    if (equals == null) {
                        return null;
                    }
                    if (equals.getDataType() == SQLite3DataType.INT && equals.asInt() == 1) {
                        return SQLite3Constant.createTrue();
                    } else {
                        SQLite3Constant applyLess = left.applyLess(right, collate);
                        if (applyLess == null) {
                            return null;
                        }
                        return UnaryOperator.NOT.apply(applyLess);
                    }
                }

            },
            EQUALS("=", "==") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    return left.applyEquals(right, collate);
                }

            },
            NOT_EQUALS("!=", "<>") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return SQLite3Constant.createNullConstant();
                    } else {
                        SQLite3Constant applyEquals = left.applyEquals(right, collate);
                        if (applyEquals == null) {
                            return null;
                        }
                        boolean equals = applyEquals.asInt() == 1;
                        return SQLite3Constant.createBoolean(!equals);
                    }
                }

            },
            IS("IS") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    } else if (left.isNull()) {
                        return SQLite3Constant.createBoolean(right.isNull());
                    } else if (right.isNull()) {
                        return SQLite3Constant.createFalse();
                    } else {
                        return left.applyEquals(right, collate);
                    }
                }

            },
            IS_NOT("IS NOT") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    } else if (left.isNull()) {
                        return SQLite3Constant.createBoolean(!right.isNull());
                    } else if (right.isNull()) {
                        return SQLite3Constant.createTrue();
                    } else {
                        SQLite3Constant applyEquals = left.applyEquals(right, collate);
                        if (applyEquals == null) {
                            return null;
                        }
                        boolean equals = applyEquals.asInt() == 1;
                        return SQLite3Constant.createBoolean(!equals);
                    }
                }

            },
            LIKE("LIKE") {
                @Override
                public boolean shouldApplyAffinity() {
                    return false;
                }

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return SQLite3Constant.createNullConstant();
                    }
                    SQLite3Constant leftStr = SQLite3Cast.castToText(left);
                    SQLite3Constant rightStr = SQLite3Cast.castToText(right);
                    if (leftStr == null || rightStr == null) {
                        return null;
                    }
                    boolean val = LikeImplementationHelper.match(leftStr.asString(), rightStr.asString(), 0, 0, false);
                    return SQLite3Constant.createBoolean(val);
                }

            },
            GLOB("GLOB") {

                @Override
                public boolean shouldApplyAffinity() {
                    return false;
                }

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return SQLite3Constant.createNullConstant();
                    }
                    SQLite3Constant leftStr = SQLite3Cast.castToText(left);
                    SQLite3Constant rightStr = SQLite3Cast.castToText(right);
                    if (leftStr == null || rightStr == null) {
                        return null;
                    }
                    boolean val = match(leftStr.asString(), rightStr.asString(), 0, 0);
                    return SQLite3Constant.createBoolean(val);
                }

                private boolean match(String str, String regex, int regexPosition, int strPosition) {
                    if (strPosition == str.length() && regexPosition == regex.length()) {
                        return true;
                    }
                    if (regexPosition >= regex.length()) {
                        return false;
                    }
                    char cur = regex.charAt(regexPosition);
                    if (strPosition >= str.length()) {
                        if (cur == '*') {
                            return match(str, regex, regexPosition + 1, strPosition);
                        } else {
                            return false;
                        }
                    }
                    switch (cur) {
                    case '[':
                        int endingBrackets = regexPosition;
                        do {
                            endingBrackets++;
                            if (endingBrackets >= regex.length()) {
                                return false;
                            }
                        } while (regex.charAt(endingBrackets) != ']');
                        StringBuilder patternInBrackets = new StringBuilder(
                                regex.substring(regexPosition + 1, endingBrackets));
                        boolean inverted;
                        if (patternInBrackets.toString().startsWith("^")) {
                            if (patternInBrackets.length() > 1) {
                                inverted = true;
                                patternInBrackets = new StringBuilder(patternInBrackets.substring(1));
                            } else {
                                return false;
                            }
                        } else {
                            inverted = false;
                        }
                        int currentSearchIndex = 0;
                        boolean found = false;
                        do {
                            int minusPosition = patternInBrackets.toString().indexOf('-', currentSearchIndex);
                            boolean minusAtBoundaries = minusPosition == 0
                                    || minusPosition == patternInBrackets.length() - 1;
                            if (minusPosition == -1 || minusAtBoundaries) {
                                break;
                            }
                            found = true;
                            StringBuilder expandedPattern = new StringBuilder();
                            for (char start = patternInBrackets.charAt(minusPosition - 1); start < patternInBrackets
                                    .charAt(minusPosition + 1); start += 1) {
                                expandedPattern.append(start);
                            }
                            patternInBrackets.replace(minusPosition, minusPosition + 1, expandedPattern.toString());
                            currentSearchIndex = minusPosition + expandedPattern.length();
                        } while (found);

                        if (patternInBrackets.length() > 0) {
                            char textChar = str.charAt(strPosition);
                            boolean contains = patternInBrackets.toString().contains(Character.toString(textChar));
                            if (contains && !inverted || !contains && inverted) {
                                return match(str, regex, endingBrackets + 1, strPosition + 1);
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }

                    case '*':
                        // match
                        boolean foundMatch = match(str, regex, regexPosition, strPosition + 1);
                        if (!foundMatch) {
                            return match(str, regex, regexPosition + 1, strPosition);
                        } else {
                            return true;
                        }
                    case '?':
                        return match(str, regex, regexPosition + 1, strPosition + 1);
                    default:
                        if (cur == str.charAt(strPosition)) {
                            return match(str, regex, regexPosition + 1, strPosition + 1);
                        } else {
                            return false;
                        }
                    }
                }

            };

            private final String[] textRepresentation;

            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, SQLite3CollateSequence collate) {
                return null;
            }

            public boolean shouldApplyAffinity() {
                return true;
            }

            BinaryComparisonOperator(String... textRepresentation) {
                this.textRepresentation = textRepresentation.clone();
            }

            public static BinaryComparisonOperator getRandomOperator() {
                return Randomly.fromOptions(values());
            }

            public static BinaryComparisonOperator getRandomRowValueOperator() {
                return Randomly.fromOptions(SMALLER, SMALLER_EQUALS, GREATER, GREATER_EQUALS, EQUALS, NOT_EQUALS);
            }

            public String getTextRepresentation() {
                return Randomly.fromOptions(textRepresentation);
            }

            public SQLite3Constant applyOperand(SQLite3Constant leftBeforeAffinity, TypeAffinity leftAffinity,
                    SQLite3Constant rightBeforeAffinity, TypeAffinity rightAffinity, SQLite3Expression origLeft,
                    SQLite3Expression origRight, boolean applyAffinity) {

                SQLite3Constant left;
                SQLite3Constant right;
                if (applyAffinity) {
                    ConstantTuple vals = applyAffinities(leftAffinity, rightAffinity, leftBeforeAffinity,
                            rightBeforeAffinity);
                    left = vals.left;
                    right = vals.right;
                } else {
                    left = leftBeforeAffinity;
                    right = rightBeforeAffinity;
                }

                // If either operand has an explicit collating function assignment using the
                // postfix COLLATE operator, then the explicit collating function is used for
                // comparison, with precedence to the collating function of the left operand.
                SQLite3CollateSequence seq = origLeft.getExplicitCollateSequence();
                if (seq == null) {
                    seq = origRight.getExplicitCollateSequence();
                }
                // If either operand is a column, then the collating function of that column is
                // used with precedence to the left operand. For the purposes of the previous
                // sentence, a column name preceded by one or more unary "+" operators is still
                // considered a column name.
                if (seq == null) {
                    seq = origLeft.getImplicitCollateSequence();
                }
                if (seq == null) {
                    seq = origRight.getImplicitCollateSequence();
                }
                // Otherwise, the BINARY collating function is used for comparison.
                if (seq == null) {
                    seq = SQLite3CollateSequence.BINARY;
                }
                return apply(left, right, seq);
            }

        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        @Override
        public String getOperatorRepresentation() {
            return operation.getTextRepresentation();
        }

    }

    public static class Sqlite3BinaryOperation extends SQLite3Expression implements BinaryOperation<SQLite3Expression> {

        public enum BinaryOperator {
            CONCATENATE("||") {
                @Override
                public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    }
                    if (!SQLite3Provider.allowFloatingPointFp && (left.getDataType() == SQLite3DataType.REAL
                            || right.getDataType() == SQLite3DataType.REAL)) {
                        throw new IgnoreMeException();
                    }
                    if (left.getExpectedValue().isNull() || right.getExpectedValue().isNull()) {
                        return SQLite3Constant.createNullConstant();
                    }
                    SQLite3Constant leftText = SQLite3Cast.castToText(left);
                    SQLite3Constant rightText = SQLite3Cast.castToText(right);
                    if (leftText == null || rightText == null) {
                        return null;
                    }
                    return SQLite3Constant.createTextConstant(leftText.asString() + rightText.asString());
                }
            },
            MULTIPLY("*") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return null;
                }

            },
            DIVIDE("/") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return null;
                }

            }, // division by zero results in zero
            REMAINDER("%") {
                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return null;
                }

            },

            PLUS("+") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return null;
                }
            },

            MINUS("-") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return null;
                }

            },
            SHIFT_LEFT("<<") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return applyIntOperation(left, right, (leftResult, rightResult) -> {
                        if (rightResult >= 0) {
                            if (rightResult >= Long.SIZE) {
                                return 0L;
                            }
                            return leftResult << rightResult;
                        } else {
                            if (rightResult == Long.MIN_VALUE) {
                                return leftResult >= 0 ? 0L : -1L;
                            }
                            return SHIFT_RIGHT.apply(left, SQLite3Constant.createIntConstant(-rightResult)).asInt();
                        }

                    });
                }

            },
            SHIFT_RIGHT(">>") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return applyIntOperation(left, right, (leftResult, rightResult) -> {
                        if (rightResult >= 0) {
                            if (rightResult >= Long.SIZE) {
                                return leftResult >= 0 ? 0L : -1L;
                            }
                            return leftResult >> rightResult;
                        } else {
                            if (rightResult == Long.MIN_VALUE) {
                                return 0L;
                            }
                            return SHIFT_LEFT.apply(left, SQLite3Constant.createIntConstant(-rightResult)).asInt();
                        }

                    });
                }

            },
            ARITHMETIC_AND("&") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return applyIntOperation(left, right, (a, b) -> a & b);
                }

            },
            ARITHMETIC_OR("|") {

                @Override
                SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    return applyIntOperation(left, right, (a, b) -> a | b);
                }

            },
            AND("AND") {

                @Override
                public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {

                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    } else {
                        Optional<Boolean> leftBoolVal = SQLite3Cast.isTrue(left.getExpectedValue());
                        Optional<Boolean> rightBoolVal = SQLite3Cast.isTrue(right.getExpectedValue());
                        if (leftBoolVal.isPresent() && !leftBoolVal.get()) {
                            return SQLite3Constant.createFalse();
                        } else if (rightBoolVal.isPresent() && !rightBoolVal.get()) {
                            return SQLite3Constant.createFalse();
                        } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                            return SQLite3Constant.createNullConstant();
                        } else {
                            return SQLite3Constant.createTrue();
                        }
                    }
                }

            },
            OR("OR") {

                @Override
                public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    } else {
                        Optional<Boolean> leftBoolVal = SQLite3Cast.isTrue(left.getExpectedValue());
                        Optional<Boolean> rightBoolVal = SQLite3Cast.isTrue(right.getExpectedValue());
                        if (leftBoolVal.isPresent() && leftBoolVal.get()) {
                            return SQLite3Constant.createTrue();
                        } else if (rightBoolVal.isPresent() && rightBoolVal.get()) {
                            return SQLite3Constant.createTrue();
                        } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                            return SQLite3Constant.createNullConstant();
                        } else {
                            return SQLite3Constant.createFalse();
                        }
                    }
                }
            };

            private final String[] textRepresentation;

            BinaryOperator(String... textRepresentation) {
                this.textRepresentation = textRepresentation.clone();
            }

            public static BinaryOperator getRandomOperator() {
                return Randomly.fromOptions(values());
            }

            public String getTextRepresentation() {
                return Randomly.fromOptions(textRepresentation);
            }

            public SQLite3Constant applyOperand(SQLite3Constant left, TypeAffinity leftAffinity, SQLite3Constant right,
                    TypeAffinity rightAffinity) {
                return apply(left, right);
            }

            public SQLite3Constant applyIntOperation(SQLite3Constant left, SQLite3Constant right,
                    java.util.function.BinaryOperator<Long> func) {
                if (left.isNull() || right.isNull()) {
                    return SQLite3Constant.createNullConstant();
                }
                SQLite3Constant leftInt = SQLite3Cast.castToInt(left);
                SQLite3Constant rightInt = SQLite3Cast.castToInt(right);
                long result = func.apply(leftInt.asInt(), rightInt.asInt());
                return SQLite3Constant.createIntConstant(result);
            }

            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return null;
            }

        }

        private final BinaryOperator operation;
        private final SQLite3Expression left;
        private final SQLite3Expression right;

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        public Sqlite3BinaryOperation(SQLite3Expression left, SQLite3Expression right, BinaryOperator operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;
        }

        public BinaryOperator getOperator() {
            return operation;
        }

        @Override
        public SQLite3Expression getLeft() {
            return left;
        }

        @Override
        public SQLite3Expression getRight() {
            return right;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                return null;
            }
            SQLite3Constant result = operation.applyOperand(left.getExpectedValue(), left.getAffinity(),
                    right.getExpectedValue(), right.getAffinity());
            if (result != null && result.isReal()) {
                SQLite3Cast.checkDoubleIsInsideDangerousRange(result.asDouble());
            }
            return result;
        }

        public static Sqlite3BinaryOperation create(SQLite3Expression leftVal, SQLite3Expression rightVal,
                BinaryOperator op) {
            return new Sqlite3BinaryOperation(leftVal, rightVal, op);
        }

        @Override
        public String getOperatorRepresentation() {
            return Randomly.fromOptions(operation.textRepresentation);
        }

    }

    public static class SQLite3ColumnName extends SQLite3Expression {

        private final SQLite3Column column;
        private final SQLite3Constant value;

        public SQLite3ColumnName(SQLite3Column name, SQLite3Constant value) {
            this.column = name;
            this.value = value;
        }

        public SQLite3Column getColumn() {
            return column;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            return value;
        }

        /*
         * When an expression is a simple reference to a column of a real table (not a VIEW or subquery) then the
         * expression has the same affinity as the table column.
         */
        @Override
        public TypeAffinity getAffinity() {
            switch (column.getType()) {
            case BINARY:
                return TypeAffinity.BLOB;
            case INT:
                return TypeAffinity.INTEGER;
            case NONE:
                return TypeAffinity.NONE;
            case REAL:
                return TypeAffinity.REAL;
            case TEXT:
                return TypeAffinity.TEXT;
            default:
                throw new AssertionError(column);
            }
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public SQLite3CollateSequence getImplicitCollateSequence() {
            return column.getCollateSequence();
        }

        public static SQLite3ColumnName createDummy(String string) {
            return new SQLite3ColumnName(SQLite3Column.createDummy(string), null);
        }

    }

    static class ConstantTuple {
        SQLite3Constant left;
        SQLite3Constant right;

        ConstantTuple(SQLite3Constant left, SQLite3Constant right) {
            this.left = left;
            this.right = right;
        }

    }

    public static ConstantTuple applyAffinities(TypeAffinity leftAffinity, TypeAffinity rightAffinity,
            SQLite3Constant leftBeforeAffinity, SQLite3Constant rightBeforeAffinity) {
        // If one operand has INTEGER, REAL or NUMERIC affinity and the other operand
        // has TEXT or BLOB or no affinity then NUMERIC affinity is applied to other
        // operand.
        SQLite3Constant left = leftBeforeAffinity;
        SQLite3Constant right = rightBeforeAffinity;
        if (leftAffinity.isNumeric() && (rightAffinity == TypeAffinity.TEXT || rightAffinity == TypeAffinity.BLOB
                || rightAffinity == TypeAffinity.NONE)) {
            right = right.applyNumericAffinity();
            assert right != null;
        } else if (rightAffinity.isNumeric() && (leftAffinity == TypeAffinity.TEXT || leftAffinity == TypeAffinity.BLOB
                || leftAffinity == TypeAffinity.NONE)) {
            left = left.applyNumericAffinity();
            assert left != null;
        }

        // If one operand has TEXT affinity and the other has no affinity, then TEXT
        // affinity is applied to the other operand.
        if (leftAffinity == TypeAffinity.TEXT && rightAffinity == TypeAffinity.NONE) {
            right = right.applyTextAffinity();
            if (right == null) {
                throw new IgnoreMeException();
            }
        } else if (rightAffinity == TypeAffinity.TEXT && leftAffinity == TypeAffinity.NONE) {
            left = left.applyTextAffinity();
            if (left == null) {
                throw new IgnoreMeException();
            }
        }
        return new ConstantTuple(left, right);
    }

    public static class SQLite3Text extends SQLite3Expression {

        private final String text;
        private final SQLite3Constant expectedValue;

        public SQLite3Text(String text, SQLite3Constant expectedValue) {
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public String getText() {
            return text;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            return expectedValue;
        }

    }

    public static class SQLite3PostfixText extends SQLite3Expression implements UnaryOperation<SQLite3Expression> {

        private final SQLite3Expression expr;
        private final String text;
        private SQLite3Constant expectedValue;

        public SQLite3PostfixText(SQLite3Expression expr, String text, SQLite3Constant expectedValue) {
            this.expr = expr;
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public SQLite3PostfixText(String text, SQLite3Constant expectedValue) {
            this(null, text, expectedValue);
        }

        public String getText() {
            return text;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            if (expr == null) {
                return null;
            } else {
                return expr.getExplicitCollateSequence();
            }
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            return expectedValue;
        }

        @Override
        public SQLite3Expression getExpression() {
            return expr;
        }

        @Override
        public String getOperatorRepresentation() {
            return getText();
        }

        @Override
        public OperatorKind getOperatorKind() {
            return OperatorKind.POSTFIX;
        }

        @Override
        public boolean omitBracketsWhenPrinting() {
            return true;
        }
    }

}
