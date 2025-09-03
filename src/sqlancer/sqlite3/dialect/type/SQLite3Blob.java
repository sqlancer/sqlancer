package sqlancer.sqlite3.dialect.type;

import sqlancer.Randomly;
import sqlancer.simple.type.Type;

public class SQLite3Blob implements Type {
    @Override
    public String instantiateRandomValue(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        byte[] bytes = r.getBytes();
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }

        return "x'" + sb + "'";
    }

    @Override
    public String toString() {
        return "BLOB";
    }
}
