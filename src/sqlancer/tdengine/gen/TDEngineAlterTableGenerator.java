package sqlancer.tdengine.gen;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.tdengine.TDEngineCommon;
import sqlancer.tdengine.TDEngineSchema.TDEngineTable;

public class TDEngineAlterTableGenerator {

	private TDEngineTable randomTable;
	private final StringBuilder sb = new StringBuilder();
	private final List<String> errors = new ArrayList<>();

	public TDEngineAlterTableGenerator(TDEngineTable randomTable) {
		this.randomTable = randomTable;
	}

	public static Query gen(TDEngineTable randomTable, Connection con, Randomly r) {
		return new TDEngineAlterTableGenerator(randomTable).generate();
	}
	
	private enum Action {
		DROP_COLUMN, ADD_COLUMN
	}

	private Query generate() {
		sb.append("ALTER TABLE ");
		sb.append(randomTable.getName());
		switch (Randomly.fromOptions(Action.values())) {
			case DROP_COLUMN:
				if (randomTable.getColumns().size() <= 2) {
					throw new IgnoreMeException();
				}
				sb.append(" DROP COLUMN ");
				sb.append(randomTable.getRandomColumn().getName());
				errors.add("primary timestamp column cannot be dropped");
				break;
			case ADD_COLUMN:
				sb.append(" ADD COLUMN ");
				sb.append(SQLite3Common.createColumnName(Randomly.smallNumber()));
				sb.append(" ");
				sb.append(TDEngineCommon.getRandomTypeString());
				errors.add("duplicated column names");
				errors.add("column length too long");
				break;
		}
		return new QueryAdapter(sb.toString(), errors) {
			@Override
			public boolean couldAffectSchema() {
				return true;
			}
		};
	}

}
