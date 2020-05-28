package sqlancer.sqlite3;

import sqlancer.Randomly;

public final class SQLite3SpecialStringGenerator {

    private SQLite3SpecialStringGenerator() {
    }

    private enum Options {
        TIME_DATE_REGEX, NOW, DATE_TIME, TIME_MODIFIER, FLOAT
    }

    public static String generate() {
        StringBuilder sb = new StringBuilder();
        switch (Randomly.fromOptions(Options.values())) {
        case TIME_DATE_REGEX: // https://www.sqlite.org/lang_datefunc.html
            return Randomly.fromOptions("%d", "%f", "%H", "%j", "%J", "%m", "%M", "%s", "%S", "%w", "%W", "%Y", "%%");
        case NOW:
            return "now";
        case DATE_TIME:
            long notCachedInteger = Randomly.getNotCachedInteger(1, 10);
            for (int i = 0; i < notCachedInteger; i++) {
                if (Randomly.getBoolean()) {
                    sb.append(Randomly.getNonCachedInteger());
                } else {
                    sb.append(Randomly.getNotCachedInteger(0, 2000));
                }
                sb.append(Randomly.fromOptions(":", "-", " ", "T"));
            }
            return sb.toString();
        case TIME_MODIFIER:
            sb.append(Randomly.fromOptions("days", "hours", "minutes", "seconds", "months", "years", "start of month",
                    "start of year", "start of day", "weekday", "unixepoch", "utc"));
            return sb.toString();
        case FLOAT:
            return Randomly.fromOptions("NaN", "Infinity", "-Infinity", "1e500000", "1-500000");
        default:
            throw new AssertionError();
        }

    }
}
