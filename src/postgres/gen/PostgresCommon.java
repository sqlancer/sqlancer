package postgres.gen;

import lama.Randomly;
import postgres.PostgresSchema.PostgresDataType;

public class PostgresCommon {
	
	private PostgresCommon() {
	}
	
	public static boolean appendDataType(PostgresDataType type, StringBuilder sb, boolean allowSerial) throws AssertionError {
		boolean serial = false;
		switch (type) {
		case BOOLEAN:
			sb.append("boolean");
			break;
		case INT:
			if (Randomly.getBoolean() && allowSerial) {
				serial = true;
				sb.append(Randomly.fromOptions("serial", "bigserial"));
			} else {
				sb.append(Randomly.fromOptions("smallint", "integer", "bigint"));
			}
			break;
		case TEXT:
			sb.append("TEXT");
			break;
		default:
			throw new AssertionError(type);
		}
		return serial;
	}

}
