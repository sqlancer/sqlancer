package sqlancer.materialize;

import java.util.List;

import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeAggregate;
import sqlancer.materialize.ast.MaterializeBetweenOperation;
import sqlancer.materialize.ast.MaterializeBinaryLogicalOperation;
import sqlancer.materialize.ast.MaterializeCastOperation;
import sqlancer.materialize.ast.MaterializeColumnValue;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeFunction;
import sqlancer.materialize.ast.MaterializeInOperation;
import sqlancer.materialize.ast.MaterializeLikeOperation;
import sqlancer.materialize.ast.MaterializeOrderByTerm;
import sqlancer.materialize.ast.MaterializePOSIXRegularExpression;
import sqlancer.materialize.ast.MaterializePostfixOperation;
import sqlancer.materialize.ast.MaterializePostfixText;
import sqlancer.materialize.ast.MaterializePrefixOperation;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeFromTable;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeSubquery;
import sqlancer.materialize.ast.MaterializeSimilarTo;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;

public interface MaterializeVisitor {

    void visit(MaterializeConstant constant);

    void visit(MaterializePostfixOperation op);

    void visit(MaterializeColumnValue c);

    void visit(MaterializePrefixOperation op);

    void visit(MaterializeSelect op);

    void visit(MaterializeOrderByTerm op);

    void visit(MaterializeFunction f);

    void visit(MaterializeCastOperation cast);

    void visit(MaterializeBetweenOperation op);

    void visit(MaterializeInOperation op);

    void visit(MaterializePostfixText op);

    void visit(MaterializeAggregate op);

    void visit(MaterializeSimilarTo op);

    void visit(MaterializePOSIXRegularExpression op);

    void visit(MaterializeFromTable from);

    void visit(MaterializeSubquery subquery);

    void visit(MaterializeBinaryLogicalOperation op);

    void visit(MaterializeLikeOperation op);

    default void visit(MaterializeExpression expression) {
        if (expression instanceof MaterializeConstant) {
            visit((MaterializeConstant) expression);
        } else if (expression instanceof MaterializePostfixOperation) {
            visit((MaterializePostfixOperation) expression);
        } else if (expression instanceof MaterializeColumnValue) {
            visit((MaterializeColumnValue) expression);
        } else if (expression instanceof MaterializePrefixOperation) {
            visit((MaterializePrefixOperation) expression);
        } else if (expression instanceof MaterializeSelect) {
            visit((MaterializeSelect) expression);
        } else if (expression instanceof MaterializeOrderByTerm) {
            visit((MaterializeOrderByTerm) expression);
        } else if (expression instanceof MaterializeFunction) {
            visit((MaterializeFunction) expression);
        } else if (expression instanceof MaterializeCastOperation) {
            visit((MaterializeCastOperation) expression);
        } else if (expression instanceof MaterializeBetweenOperation) {
            visit((MaterializeBetweenOperation) expression);
        } else if (expression instanceof MaterializeInOperation) {
            visit((MaterializeInOperation) expression);
        } else if (expression instanceof MaterializeAggregate) {
            visit((MaterializeAggregate) expression);
        } else if (expression instanceof MaterializePostfixText) {
            visit((MaterializePostfixText) expression);
        } else if (expression instanceof MaterializeSimilarTo) {
            visit((MaterializeSimilarTo) expression);
        } else if (expression instanceof MaterializePOSIXRegularExpression) {
            visit((MaterializePOSIXRegularExpression) expression);
        } else if (expression instanceof MaterializeFromTable) {
            visit((MaterializeFromTable) expression);
        } else if (expression instanceof MaterializeSubquery) {
            visit((MaterializeSubquery) expression);
        } else if (expression instanceof MaterializeLikeOperation) {
            visit((MaterializeLikeOperation) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

    static String asString(MaterializeExpression expr) {
        MaterializeToStringVisitor visitor = new MaterializeToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(MaterializeExpression expr) {
        MaterializeExpectedValueVisitor v = new MaterializeExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

    static String getExpressionAsString(MaterializeGlobalState globalState, MaterializeDataType type,
            List<MaterializeColumn> columns) {
        MaterializeExpression expression = MaterializeExpressionGenerator.generateExpression(globalState, columns,
                type);
        MaterializeToStringVisitor visitor = new MaterializeToStringVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

}
