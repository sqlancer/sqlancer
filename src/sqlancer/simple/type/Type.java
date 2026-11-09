package sqlancer.simple.type;

import sqlancer.Randomly;

public interface Type {
    String instantiateRandomValue(Randomly r);
}
