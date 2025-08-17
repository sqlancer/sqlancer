package sqlancer.common.log;

public class LoggedString implements Loggable {
    private static final long serialVersionUID = 1L;

    private final String loggedString;

    public LoggedString(String loggedString) {
        this.loggedString = loggedString;
    }

    @Override
    public String getLogString() {
        return this.loggedString;
    }
}
