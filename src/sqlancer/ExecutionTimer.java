package sqlancer;

import java.util.concurrent.atomic.AtomicLong;

public final class ExecutionTimer {

    private long startTime = -1;
    private long endTime = -1;

    /**
     * Starts the timer.
     * @return the current ExecutionTimer instance.
     */
    public ExecutionTimer start() {
        startTime = System.nanoTime(); // Use nanoTime for higher precision
        endTime = -1; // Reset endTime to allow reuse
        return this;
    }

    /**
     * Stops the timer.
     * @return the current ExecutionTimer instance.
     * @throws IllegalStateException if the timer was not started.
     */
    public ExecutionTimer end() {
        if (startTime == -1) {
            throw new IllegalStateException("Timer has not been started.");
        }
        endTime = System.nanoTime();
        return this;
    }

    /**
     * Returns the elapsed time as a formatted string.
     * @return a string representing the time in milliseconds and nanoseconds.
     * @throws IllegalStateException if the timer was not started or stopped properly.
     */
    public String asString() {
        if (startTime == -1) {
            throw new IllegalStateException("Timer has not been started.");
        }
        if (endTime == -1) {
            throw new IllegalStateException("Timer has not been stopped.");
        }
        long durationNanos = endTime - startTime;
        return (durationNanos / 1_000_000) + "ms (" + durationNanos + "ns)";
    }

    /**
     * Returns the elapsed time in milliseconds.
     * @return the elapsed time in milliseconds.
     * @throws IllegalStateException if the timer was not started or stopped properly.
     */
    public long getElapsedMillis() {
        if (startTime == -1 || endTime == -1) {
            throw new IllegalStateException("Timer has not been properly started or stopped.");
        }
        return (endTime - startTime) / 1_000_000;
    }

    /**
     * Returns the elapsed time in nanoseconds.
     * @return the elapsed time in nanoseconds.
     * @throws IllegalStateException if the timer was not started or stopped properly.
     */
    public long getElapsedNanos() {
        if (startTime == -1 || endTime == -1) {
            throw new IllegalStateException("Timer has not been properly started or stopped.");
        }
        return endTime - startTime;
    }

    /**
     * Resets the timer, allowing it to be reused.
     */
    public void reset() {
        startTime = -1;
        endTime = -1;
    }
}
