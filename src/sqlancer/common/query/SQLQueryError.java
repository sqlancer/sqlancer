package sqlancer.common.query;

import java.util.Objects;

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
        return Objects.equals(level, that.getLevel());
    }

    public boolean hasSameCodeAndMessage(SQLQueryError that) {
        if (code != that.getCode()) {
            return false;
        }
        return Objects.equals(message, that.getMessage());
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

    @Override
    public int hashCode() {
        return Objects.hash(level, code, message);
    }
}
