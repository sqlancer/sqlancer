package sqlancer.mysql.ast;

public class MySQLText implements MySQLExpression {

    private final String text;

    public MySQLText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
