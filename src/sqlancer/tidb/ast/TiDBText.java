package sqlancer.tidb.ast;

public class TiDBText implements TiDBExpression {

    private final String text;

    public TiDBText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
