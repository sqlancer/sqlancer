package lama.cockroachdb.ast;

import lama.cockroachdb.CockroachDBSchema.CockroachDBColumn;

public class CockroachDBColumnReference extends CockroachDBExpression {
	
	private final CockroachDBColumn c;

	public CockroachDBColumnReference(CockroachDBColumn c) {
		this.c = c;
	}
	
	public CockroachDBColumn getColumn() {
		return c;
	}

}
