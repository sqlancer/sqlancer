package lama.cockroachdb;

import java.util.ArrayList;
import java.util.List;

import lama.Randomly;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTable;
import lama.cockroachdb.ast.CockroachDBIndexReference;
import lama.cockroachdb.ast.CockroachDBTableReference;

public class CockroachDBCommon {
	
	public static String getRandomCollate() {
		return Randomly.fromOptions("en", "de", "es", "cmn");
	}
	
	public static List<CockroachDBTableReference> getTableReferences(List<CockroachDBTableReference> tableList) {
		List<CockroachDBTableReference> from = new ArrayList<>();
		for (CockroachDBTableReference t : tableList) {
			CockroachDBTable table = t.getTable();
			if (!table.getIndexes().isEmpty() && Randomly.getBooleanWithSmallProbability()) {
				from.add(new CockroachDBIndexReference(t, table.getRandomIndex()));
			} else {
				from.add(t);
			}
		}
		return from;
	}

}
