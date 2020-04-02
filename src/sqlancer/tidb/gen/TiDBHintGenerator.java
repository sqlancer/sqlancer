package sqlancer.tidb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.schema.TableIndex;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBText;

public class TiDBHintGenerator {
	
	enum IndexHint {
		USE_INDEX_MERGE;
	}

	public static void generateHints(TiDBSelect select, List<TiDBTable> tables) {
		TiDBTable table = Randomly.fromList(tables);
		switch (Randomly.fromOptions(IndexHint.values())) {
		case USE_INDEX_MERGE:
			if (true) {
				// https://github.com/pingcap/tidb/issues/15994
				// https://github.com/pingcap/tidb/issues/15992
				// https://github.com/pingcap/tidb/issues/15991
				throw new IgnoreMeException();
			}
			if (table.hasIndexes()) {
				StringBuilder sb = new StringBuilder("USE_INDEX_MERGE(");
				sb.append(table.getName());
				sb.append(", ");
				List<TableIndex> indexes = Randomly.nonEmptySubset(table.getIndexes());
				sb.append(indexes.stream().map(i -> i.getIndexName()).collect(Collectors.joining(", ")));
				sb.append(")");
				select.setHint(new TiDBText(sb.toString()));
			} else {
				throw new IgnoreMeException();
			}
			
		}
	}

}
