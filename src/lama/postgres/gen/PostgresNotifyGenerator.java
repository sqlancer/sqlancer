package lama.postgres.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

public class PostgresNotifyGenerator {

	private static String getChannel() {
		return Randomly.fromOptions("asdf", "test");
	}

	public static Query createNotify(Randomly r) {
		StringBuilder sb = new StringBuilder();
		sb.append("NOTIFY ");
		sb.append(getChannel());
		if (Randomly.getBoolean()) {
			sb.append(", ");
			sb.append("'");
			sb.append(r.getString().replace("'", "''"));
			sb.append("'");
		}
		return new QueryAdapter(sb.toString());
	}

	public static Query createListen() {
		StringBuilder sb = new StringBuilder();
		sb.append("LISTEN ");
		sb.append(getChannel());
		return new QueryAdapter(sb.toString());
	}

	public static Query createUnlisten() {
		StringBuilder sb = new StringBuilder();
		sb.append("UNLISTEN ");
		if (Randomly.getBoolean()) {
			sb.append(getChannel());
		} else {
			sb.append("*");
		}
		return new QueryAdapter(sb.toString());
	}

}
