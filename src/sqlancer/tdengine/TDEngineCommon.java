package sqlancer.tdengine;

import sqlancer.Randomly;

public class TDEngineCommon {

	enum Types {
		TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BOOL, TIMESTAMP, BINARY, NCHAR;
	}
	
	public static String getRandomTypeString() {
		Types randomType = Randomly.fromOptions(Types.values());
		switch (randomType) {
		case BINARY:
		case NCHAR:
			int size = (int) Randomly.getNotCachedInteger(1, 4096);
			return String.format("%s(%d)", randomType.toString(), size);
		default:
			return randomType.toString();
		}
	}

}
