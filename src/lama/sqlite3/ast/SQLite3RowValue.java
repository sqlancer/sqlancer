package lama.sqlite3.ast;

import java.util.List;

import lama.sqlite3.schema.SQLite3Schema.SQLite3Column.CollateSequence;

public class SQLite3RowValue extends SQLite3Expression {
	
	private final List<SQLite3Expression> expressions;

	public SQLite3RowValue(List<SQLite3Expression> expressions) {
		this.expressions = expressions;
	}

	public List<SQLite3Expression> getExpressions() {
		return expressions;
	}

	@Override
	public CollateSequence getExplicitCollateSequence() {
		for (SQLite3Expression expr : expressions) {
			CollateSequence collate = expr.getExplicitCollateSequence();
			if (collate != null) {
				return collate;
			}
		}
		return null;
	}
	
}
