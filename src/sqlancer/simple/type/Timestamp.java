package sqlancer.simple.type;

import java.text.SimpleDateFormat;

import sqlancer.Randomly;

public class Timestamp implements Type {

    @Override
    public String instantiateRandomValue(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }

        long val = r.getInteger();
        java.sql.Timestamp timestamp = new java.sql.Timestamp(val);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return "TIMESTAMP '" + dateFormat.format(timestamp) + "'";
    }

    @Override
    public String toString() {
        return "TIMESTAMP";
    }

}
