package sqlancer.presto;

public class PrestoPair<T, U> {
    private final T first;
    private final U second;

    public PrestoPair(T first, U second) {
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
