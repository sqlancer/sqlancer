package sqlancer.simple.type;

import sqlancer.Randomly;

public class Varchar implements Type {
    @Override
    public String instantiateRandomValue(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        String text = r.getString();

        return "'" + text.replace("'", "''") + "'";
    }

    @Override
    public String toString() {
        return "VARCHAR";
    }
}
