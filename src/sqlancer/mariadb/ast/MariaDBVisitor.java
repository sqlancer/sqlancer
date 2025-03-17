package sqlancer.mariadb.ast;

public interface MariaDBVisitor {

    void visit(MariaDBConstant c);

    void visit(MariaDBPostfixUnaryOperation op);

    void visit(MariaDBColumnName c);

    void visit(MariaDBSelectStatement s);

    void visit(MariaDBText t);

    void visit(MariaDBAggregate aggr);

    void visit(MariaDBBinaryOperator comp);

    void visit(MariaDBUnaryPrefixOperation op);

    void visit(MariaDBFunction func);

    void visit(MariaDBInOperation op);

    void visit(MariaDBJoin join);

    void visit(MariaDBTableReference join);

    default void visit(MariaDBExpression expr) {
        if (expr instanceof MariaDBConstant) {
            visit((MariaDBConstant) expr);
        } else if (expr instanceof MariaDBColumnName) {
            visit((MariaDBColumnName) expr);
        } else if (expr instanceof MariaDBSelectStatement) {
            visit((MariaDBSelectStatement) expr);
        } else if (expr instanceof MariaDBPostfixUnaryOperation) {
            visit((MariaDBPostfixUnaryOperation) expr);
        } else if (expr instanceof MariaDBText) {
            visit((MariaDBText) expr);
        } else if (expr instanceof MariaDBAggregate) {
            visit((MariaDBAggregate) expr);
        } else if (expr instanceof MariaDBBinaryOperator) {
            visit((MariaDBBinaryOperator) expr);
        } else if (expr instanceof MariaDBUnaryPrefixOperation) {
            visit((MariaDBUnaryPrefixOperation) expr);
        } else if (expr instanceof MariaDBFunction) {
            visit((MariaDBFunction) expr);
        } else if (expr instanceof MariaDBInOperation) {
            visit((MariaDBInOperation) expr);
        } else if (expr instanceof MariaDBJoin) {
            visit((MariaDBJoin) expr);
        } else if (expr instanceof MariaDBTableReference) {
            visit((MariaDBTableReference) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    static String asString(MariaDBExpression expr) {
        MariaDBStringVisitor v = new MariaDBStringVisitor();
        v.visit(expr);
        return v.getString();
    }

}
