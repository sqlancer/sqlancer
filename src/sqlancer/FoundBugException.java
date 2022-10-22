package sqlancer;

import sqlancer.sqlite3.SQLite3GlobalState;

public class FoundBugException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Reproducer reproducer;

    public interface Reproducer {
        boolean bugStillTriggers(SQLite3GlobalState globalState);

        default void outputHook(SQLite3GlobalState globalState) {

        }
    }

    public FoundBugException(String string, Reproducer reproducer) {
        super(string);
        this.reproducer = reproducer;
    }

    public Reproducer getReproducer() {
        return reproducer;
    }

}
