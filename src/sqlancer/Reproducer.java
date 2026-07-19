package sqlancer;

public interface Reproducer<G extends GlobalState<?, ?, ?>> {
    boolean bugStillTriggers(G globalState);

    /**
     * Describes how to trigger the bug on the database set up by the reduced statements (e.g., the oracle queries to
     * run and the failure to expect), so that the reduced test case is complete without the reproducer object.
     *
     * @return a human-readable description, or null if the reproducer does not provide one
     */
    default String getBugInformation() {
        return null;
    }
}
