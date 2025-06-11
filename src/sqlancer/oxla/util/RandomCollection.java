package sqlancer.oxla.util;

import sqlancer.Randomly;

import java.util.NavigableMap;
import java.util.TreeMap;

public class RandomCollection<E> {
    private final NavigableMap<Integer, E> map = new TreeMap<>();
    private int total = 0;

    public RandomCollection<E> add(int weight, E result) {
        if (weight <= 0) {
            return this;
        }
        total += weight;
        map.put(total, result);
        return this;
    }

    public RandomCollection<E> add(int weight, E result, boolean condition) {
        if (!condition) {
            return this;
        }
        return add(weight, result);
    }

    public E getRandom() {
        final long value = Randomly.getNotCachedInteger(0, total + 1);
        final NavigableMap.Entry<Integer, E> entry = map.higherEntry((int)value);
        if (entry != null) {
            return entry.getValue();
        }
        return map.firstEntry().getValue();
    }
}

