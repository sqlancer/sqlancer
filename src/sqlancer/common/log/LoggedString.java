package sqlancer.common.log;

public class LoggedString implements Loggable {

    private final String loggedString;

    public LoggedString(String loggedString) {
        this.loggedString = loggedString;
    }

    @Override
    public String getLogString() {
        return this.loggedString;
    }
}
