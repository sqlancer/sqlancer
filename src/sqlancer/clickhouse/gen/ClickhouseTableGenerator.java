package sqlancer.clickhouse.gen;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickhouseProvider.ClickhouseGlobalState;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseDataType;

public class ClickhouseTableGenerator {

	private enum ClickhouseEngine {
		TinyLog, StripeLog, Log, Memory/*, MergeTree*/
	}

	StringBuilder sb = new StringBuilder("CREATE TABLE ");

	public Query getQuery(ClickhouseGlobalState globalState) {
		ClickhouseEngine engine = Randomly.fromOptions(ClickhouseEngine.values());
		sb.append(globalState.getSchema().getFreeTableName());
		sb.append("(");
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("c");
			sb.append(i);
			sb.append(" ");
			sb.append(ClickhouseDataType.getRandom());
			potentiallyAppendCodec();
		}
		sb.append(") ENGINE = ");
		sb.append(engine);
		sb.append("(");
		switch (engine) {
		case Log:
		case Memory:
		case StripeLog:
		case TinyLog:
			break;
//		case MergeTree:
//			sb.append("toDate(c0), (c0), 8192");
//			break;
		default:
			throw new AssertionError(engine);
		}
		sb.append(")");
		sb.append(";");
		return new QueryAdapter(sb.toString());
	}

	private void potentiallyAppendCodec() {
		if (Randomly.getBoolean()) {
			sb.append(" CODEC(");
			sb.append(Randomly.fromOptions("NONE", "ZSTD", "LZ4HC", "Delta"));
			sb.append(")");
		}
	}

}
