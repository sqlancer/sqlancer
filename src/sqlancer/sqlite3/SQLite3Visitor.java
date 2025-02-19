package sqlancer.sqlite3;

import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Case.SQLite3CaseWithBaseExpression;
import sqlancer.sqlite3.ast.SQLite3Case.SQLite3CaseWithoutBaseExpression;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3BetweenOperation;
import sqlancer.sqlite3.ast.SQLite3BinaryComparisonOperation;
import sqlancer.sqlite3.ast.SQLite3ExpressionCast;
import sqlancer.sqlite3.ast.SQLite3CollateOperation;
import sqlancer.sqlite3.ast.SQLite3ExpressionFunction;
import sqlancer.sqlite3.ast.SQLite3Function;
import sqlancer.sqlite3.ast.SQLite3InOperation;
import sqlancer.sqlite3.ast.SQLite3Join;
import sqlancer.sqlite3.ast.SQLite3MatchOperation;
import sqlancer.sqlite3.ast.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Distinct;
import sqlancer.sqlite3.ast.SQLite3Exist;
import sqlancer.sqlite3.ast.SQLite3OrderingTerm;
import sqlancer.sqlite3.ast.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3TableReference;
import sqlancer.sqlite3.ast.SQLite3Text;
import sqlancer.sqlite3.ast.SQLite3BinaryOperation;
import sqlancer.sqlite3.ast.SQLite3Subquery;
import sqlancer.sqlite3.ast.SQLite3TypeLiteral;
import sqlancer.sqlite3.ast.SQLite3RowValueExpression;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.ast.SQLite3SetClause;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation;
import sqlancer.sqlite3.ast.SQLite3WindowFunction;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecBetween;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecTerm;

public interface SQLite3Visitor {

    static byte[] hexStringToByteArray(String s) {
        byte[] b = new byte[s.length() / 2];
        for (int i = 0; i < b.length; i++) {
            int index = i * 2;
            int v = Integer.parseInt(s.substring(index, index + 2), 16);
            b[i] = (byte) v;
        }
        return b;
    }

    static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // TODO remove these default methods

    default void visit(SQLite3BinaryComparisonOperation op) {

    }

    default void visit(SQLite3BinaryOperation op) {

    }

    default void visit(SQLite3UnaryOperation exp) {

    }

    default void visit(SQLite3PostfixText op) {

    }

    default void visit(SQLite3PostfixUnaryOperation exp) {

    }

    void visit(SQLite3BetweenOperation op);

    void visit(SQLite3ColumnName c);

    void visit(SQLite3Constant c);

    void visit(SQLite3ExpressionFunction f);

    void visit(SQLite3Select s, boolean inner);

    void visit(SQLite3OrderingTerm term);

    void visit(SQLite3TableReference tableReference);

    void visit(SQLite3SetClause set);

    void visit(SQLite3CollateOperation op);

    void visit(SQLite3ExpressionCast cast);

    void visit(SQLite3TypeLiteral literal);

    void visit(SQLite3InOperation op);

    void visit(SQLite3Subquery query);

    void visit(SQLite3Exist exist);

    void visit(SQLite3Join join);

    void visit(SQLite3MatchOperation match);

    void visit(SQLite3Function func);

    void visit(SQLite3Text func);

    void visit(SQLite3Distinct distinct);

    void visit(SQLite3CaseWithoutBaseExpression casExpr);

    void visit(SQLite3CaseWithBaseExpression casExpr);

    void visit(SQLite3Aggregate aggr);

    void visit(SQLite3WindowFunction func);

    void visit(SQLite3RowValueExpression rw);

    void visit(SQLite3WindowFunctionExpression windowFunction);

    void visit(SQLite3WindowFunctionFrameSpecTerm term);

    void visit(SQLite3WindowFunctionFrameSpecBetween between);

    default void visit(SQLite3Expression expr) {
        if (expr instanceof SQLite3BinaryOperation) {
            visit((SQLite3BinaryOperation) expr);
        } else if (expr instanceof SQLite3ColumnName) {
            visit((SQLite3ColumnName) expr);
        } else if (expr instanceof SQLite3Constant) {
            visit((SQLite3Constant) expr);
        } else if (expr instanceof SQLite3UnaryOperation) {
            visit((SQLite3UnaryOperation) expr);
        } else if (expr instanceof SQLite3PostfixUnaryOperation) {
            visit((SQLite3PostfixUnaryOperation) expr);
        } else if (expr instanceof SQLite3ExpressionFunction) {
            visit((SQLite3ExpressionFunction) expr);
        } else if (expr instanceof SQLite3BetweenOperation) {
            visit((SQLite3BetweenOperation) expr);
        } else if (expr instanceof SQLite3CollateOperation) {
            visit((SQLite3CollateOperation) expr);
        } else if (expr instanceof SQLite3OrderingTerm) {
            visit((SQLite3OrderingTerm) expr);
        } else if (expr instanceof SQLite3InOperation) {
            visit((SQLite3InOperation) expr);
        } else if (expr instanceof SQLite3ExpressionCast) {
            visit((SQLite3ExpressionCast) expr);
        } else if (expr instanceof SQLite3Subquery) {
            visit((SQLite3Subquery) expr);
        } else if (expr instanceof SQLite3Join) {
            visit((SQLite3Join) expr);
        } else if (expr instanceof SQLite3Select) {
            visit((SQLite3Select) expr, true);
        } else if (expr instanceof SQLite3Exist) {
            visit((SQLite3Exist) expr);
        } else if (expr instanceof SQLite3BinaryComparisonOperation) {
            visit((SQLite3BinaryComparisonOperation) expr);
        } else if (expr instanceof SQLite3Function) {
            visit((SQLite3Function) expr);
        } else if (expr instanceof SQLite3Distinct) {
            visit((SQLite3Distinct) expr);
        } else if (expr instanceof SQLite3CaseWithoutBaseExpression) {
            visit((SQLite3CaseWithoutBaseExpression) expr);
        } else if (expr instanceof SQLite3CaseWithBaseExpression) {
            visit((SQLite3CaseWithBaseExpression) expr);
        } else if (expr instanceof SQLite3Aggregate) {
            visit((SQLite3Aggregate) expr);
        } else if (expr instanceof SQLite3PostfixText) {
            visit((SQLite3PostfixText) expr);
        } else if (expr instanceof SQLite3WindowFunction) {
            visit((SQLite3WindowFunction) expr);
        } else if (expr instanceof SQLite3MatchOperation) {
            visit((SQLite3MatchOperation) expr);
        } else if (expr instanceof SQLite3RowValueExpression) {
            visit((SQLite3RowValueExpression) expr);
        } else if (expr instanceof SQLite3Text) {
            visit((SQLite3Text) expr);
        } else if (expr instanceof SQLite3WindowFunctionExpression) {
            visit((SQLite3WindowFunctionExpression) expr);
        } else if (expr instanceof SQLite3WindowFunctionFrameSpecTerm) {
            visit((SQLite3WindowFunctionFrameSpecTerm) expr);
        } else if (expr instanceof SQLite3WindowFunctionFrameSpecBetween) {
            visit((SQLite3WindowFunctionFrameSpecBetween) expr);
        } else if (expr instanceof SQLite3TableReference) {
            visit((SQLite3TableReference) expr);
        } else if (expr instanceof SQLite3SetClause) {
            visit((SQLite3SetClause) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(SQLite3Expression expr) {
        if (expr == null) {
            throw new AssertionError();
        }
        SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
        if (expr instanceof SQLite3Select) {
            visitor.visit((SQLite3Select) expr, false);
        } else {
            visitor.visit(expr);
        }
        return visitor.get();
    }

    static String asExpectedValues(SQLite3Expression expr) {
        SQLite3ExpectedValueVisitor visitor = new SQLite3ExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
