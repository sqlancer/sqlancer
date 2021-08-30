package sqlancer;

import sqlancer.sqlite3.SQLite3GlobalState;

public class FoundBugException extends RuntimeException {

    public interface Reproducer {
        public abstract boolean bugStillTriggers(SQLite3GlobalState globalState);

        public default void outputHook(SQLite3GlobalState globalState) {

        }
    }

    private static final long serialVersionUID = 1L;
    private Reproducer reproducer;

    public FoundBugException(String string, Reproducer reproducer) {
        super(string);
        this.reproducer = reproducer;
    }

    public Reproducer getReproducer() {
        return reproducer;
    }

}
