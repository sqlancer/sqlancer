package sqlancer.simple.type;

import sqlancer.Randomly;

public class BigInt implements Type {
    @Override
    public String instantiateRandomValue(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        return String.valueOf(r.getInteger());
    }

    @Override
    public String toString() {
        return "BIGINT";
    }
}
