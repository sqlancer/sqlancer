package lama.cockroachdb.ast;

import lama.cockroachdb.CockroachDBSchema.CockroachDBTable;

public class CockroachDBTableReference extends CockroachDBExpression {

	private final CockroachDBTable table;

	public CockroachDBTableReference(CockroachDBTable table) {
		this.table = table;
	}
	
	public CockroachDBTable getTable() {
		return table;
	}
	
}
