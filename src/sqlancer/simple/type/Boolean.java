package sqlancer.simple.type;

import sqlancer.Randomly;

public class Boolean implements Type {
    @Override
    public String instantiateRandomValue(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        return String.valueOf(Randomly.getBoolean());
    }

    @Override
    public String toString() {
        return "BOOLEAN";
    }
}
