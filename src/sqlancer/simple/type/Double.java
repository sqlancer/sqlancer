package sqlancer.simple.type;

import sqlancer.Randomly;

public class Double implements Type {
    @Override
    public String instantiateRandomValue(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }

        double val = r.getDouble();
        if (val == java.lang.Double.POSITIVE_INFINITY) {
            return "'+Inf'";
        } else if (val == java.lang.Double.NEGATIVE_INFINITY) {
            return "'-Inf'";
        }

        return String.valueOf(val);
    }

    @Override
    public String toString() {
        return "DOUBLE";
    }
}
