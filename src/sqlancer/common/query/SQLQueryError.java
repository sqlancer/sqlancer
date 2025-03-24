package sqlancer.common.query;

public class SQLQueryError implements Comparable<SQLQueryError> {

    public enum ErrorLevel {
        WARNING, ERROR
    }

    private ErrorLevel level;
    private int code;
    private String message;

    public void setLevel(String level) {
        // value of is case-sensitive
        this.level = ErrorLevel.valueOf(level.toUpperCase());
    }

    public void setCode(int code) {
        this.code = code;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ErrorLevel getLevel() {
        return level;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public boolean hasSameLevel(SQLQueryError that) {
        if (level == null && that.getLevel() == null) {
            return true;
        } else if (level != null && level.equals(that.getLevel())) {
            return true;
        } else {
            return false;
        }
    }

    public boolean hasSameCodeAndMessage(SQLQueryError that) {
        if (code != that.getCode()) {
            return false;
        }
        if (message == null && that.getMessage() == null) {
            return true;
        } else if (message != null && message.equals(that.getMessage())) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) {
            return false;
        }
        if (that instanceof SQLQueryError) {
            SQLQueryError thatError = (SQLQueryError) that;
            if (hasSameLevel(thatError) && hasSameCodeAndMessage(thatError)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("Level: %s; Code: %d; Message: %s.", level, code, message);
    }

    @Override
    public int compareTo(SQLQueryError that) {
        if (code < that.getCode()) {
            return -1;
        } else if (code > that.getCode()) {
            return 1;
        }

        if (level == null && that.getLevel() != null) {
            return -1;
        }
        if (level != null && that.getLevel() == null) {
            return 1;
        }
        if (level != null && that.getLevel() != null) {
            int res = level.compareTo(that.getLevel());
            if (res != 0) {
                return res;
            }
        }

        if (message == null && that.getMessage() != null) {
            return -1;
        }
        if (message != null && that.getMessage() == null) {
            return 1;
        }
        if (message != null && that.getMessage() != null) {
            int res = message.compareTo(that.getMessage());
            if (res != 0) {
                return res;
            }
        }

        return 0;
    }
}

