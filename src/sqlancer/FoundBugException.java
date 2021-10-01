package sqlancer;

import sqlancer.common.schema.AbstractSchema;

public class FoundBugException extends RuntimeException {

    public interface Reproducer<G extends SQLGlobalState<O, ? extends AbstractSchema<G , ?>>, O extends DBMSSpecificOptions<?>> {
        public abstract boolean bugStillTriggers(G globalState);

        public default void outputHook(G globalState) {

        }
    }

    private static final long serialVersionUID = 1L;
    private final Reproducer reproducer;

    public FoundBugException(String string, Reproducer reproducer) {
        super(string);
        this.reproducer = reproducer;
    }

    public Reproducer getReproducer() {
        return reproducer;
    }

}
