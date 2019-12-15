package lama.mariadb.ast;

public class MariaDBText extends MariaDBExpression {

	private MariaDBExpression expr;
	private String text;
	private boolean prefix;

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
