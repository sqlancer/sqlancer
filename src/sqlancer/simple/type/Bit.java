package sqlancer.simple.type;

import sqlancer.Randomly;

public class Bit implements Type {
    @Override
    public String instantiateRandomValue(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        String bits = Long.toBinaryString(r.getInteger());

        return "B'" + bits + "'";
    }

    @Override
    public String toString() {
        return "BIT";
    }
}
