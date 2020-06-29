package sqlancer.mariadb.ast;

public class MariaDBText extends MariaDBExpression {

    private final MariaDBExpression expr;
    private final String text;
    private final boolean prefix;

    public MariaDBText(MariaDBExpression expr, String text, boolean prefix) {
        this.expr = expr;
        this.text = text;
        this.prefix = prefix;
    }

    public MariaDBExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    public boolean isPrefix() {
        return prefix;
    }
}
