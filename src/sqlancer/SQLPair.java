package sqlancer;

public class SQLPair<T, U> {
    private final T first;
    private final U second;

    public SQLPair(T first, U second) {
        this.first = first;
        this.second = second;
    }

    public T getFirstValue() {
        return first;
    }

    public U getSecondValue() {
        return second;
    }

    @Override
    public String toString() {
        return "Pair[" + first + ", " + second + "]";
    }
}
