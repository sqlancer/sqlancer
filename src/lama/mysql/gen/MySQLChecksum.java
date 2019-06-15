package lama.mysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.mysql.MySQLSchema.MySQLTable;

public class MySQLChecksum {

	private final List<MySQLTable> tables;
	private final StringBuilder sb = new StringBuilder();

	public MySQLChecksum(List<MySQLTable> tables) {
		this.tables = tables;
	}

	public static Query checksum(List<MySQLTable> tables) {
		return new MySQLChecksum(tables).checksum();
	}

	private Query checksum() {
		sb.append("CHECKSUM TABLE ");
		sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
		return new QueryAdapter(sb.toString());
	}

}
