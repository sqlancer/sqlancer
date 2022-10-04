package sqlancer.yugabyte.ysql;

import java.util.List;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.ast.YSQLAggregate;
import sqlancer.yugabyte.ysql.ast.YSQLBetweenOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryLogicalOperation;
import sqlancer.yugabyte.ysql.ast.YSQLCastOperation;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLFunction;
import sqlancer.yugabyte.ysql.ast.YSQLInOperation;
import sqlancer.yugabyte.ysql.ast.YSQLOrderByTerm;
import sqlancer.yugabyte.ysql.ast.YSQLPOSIXRegularExpression;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixText;
import sqlancer.yugabyte.ysql.ast.YSQLPrefixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.YSQLFromTable;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.YSQLSubquery;
import sqlancer.yugabyte.ysql.ast.YSQLSimilarTo;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;

public interface YSQLVisitor {

    static String asString(YSQLExpression expr) {
        YSQLToStringVisitor visitor = new YSQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(YSQLExpression expr) {
        YSQLExpectedValueVisitor v = new YSQLExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

    static String getExpressionAsString(YSQLGlobalState globalState, YSQLDataType type, List<YSQLColumn> columns) {
        YSQLExpression expression = YSQLExpressionGenerator.generateExpression(globalState, columns, type);
        YSQLToStringVisitor visitor = new YSQLToStringVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

    void visit(YSQLConstant constant);

    void visit(YSQLPostfixOperation op);

    void visit(YSQLColumnValue c);

    void visit(YSQLPrefixOperation op);

    void visit(YSQLSelect op);

    void visit(YSQLOrderByTerm op);

    void visit(YSQLFunction f);

    void visit(YSQLCastOperation cast);

    void visit(YSQLBetweenOperation op);

    void visit(YSQLInOperation op);

    void visit(YSQLPostfixText op);

    void visit(YSQLAggregate op);

    void visit(YSQLSimilarTo op);

    void visit(YSQLPOSIXRegularExpression op);

    void visit(YSQLFromTable from);

    void visit(YSQLSubquery subquery);

    void visit(YSQLBinaryLogicalOperation op);

    default void visit(YSQLExpression expression) {
        if (expression instanceof YSQLConstant) {
            visit((YSQLConstant) expression);
        } else if (expression instanceof YSQLPostfixOperation) {
            visit((YSQLPostfixOperation) expression);
        } else if (expression instanceof YSQLColumnValue) {
            visit((YSQLColumnValue) expression);
        } else if (expression instanceof YSQLPrefixOperation) {
            visit((YSQLPrefixOperation) expression);
        } else if (expression instanceof YSQLSelect) {
            visit((YSQLSelect) expression);
        } else if (expression instanceof YSQLOrderByTerm) {
            visit((YSQLOrderByTerm) expression);
        } else if (expression instanceof YSQLFunction) {
            visit((YSQLFunction) expression);
        } else if (expression instanceof YSQLCastOperation) {
            visit((YSQLCastOperation) expression);
        } else if (expression instanceof YSQLBetweenOperation) {
            visit((YSQLBetweenOperation) expression);
        } else if (expression instanceof YSQLInOperation) {
            visit((YSQLInOperation) expression);
        } else if (expression instanceof YSQLAggregate) {
            visit((YSQLAggregate) expression);
        } else if (expression instanceof YSQLPostfixText) {
            visit((YSQLPostfixText) expression);
        } else if (expression instanceof YSQLSimilarTo) {
            visit((YSQLSimilarTo) expression);
        } else if (expression instanceof YSQLPOSIXRegularExpression) {
            visit((YSQLPOSIXRegularExpression) expression);
        } else if (expression instanceof YSQLFromTable) {
            visit((YSQLFromTable) expression);
        } else if (expression instanceof YSQLSubquery) {
            visit((YSQLSubquery) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

}
