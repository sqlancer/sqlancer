package sqlancer.simple.type;

import sqlancer.Randomly;

public class BigInt implements Type {
    @Override
    public String instantiate() {
        return String.valueOf(Randomly.getNonCachedInteger());
    }

    @Override
    public String toString() {
        return "BIGINT";
    }
}
