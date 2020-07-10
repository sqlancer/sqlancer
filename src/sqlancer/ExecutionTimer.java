package sqlancer;

public final class ExecutionTimer {

    private long startTime;
    private long endTime;

    public ExecutionTimer start() {
        startTime = System.currentTimeMillis();
        return this;
    }

    public ExecutionTimer end() {
        endTime = System.currentTimeMillis();
        return this;
    }

    public String asString() {
        long timeMillis = endTime - startTime;
        return timeMillis + "ms";
    }

}
