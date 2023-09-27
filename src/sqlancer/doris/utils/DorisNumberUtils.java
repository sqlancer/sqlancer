package sqlancer.doris.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DorisNumberUtils {
    private static Pattern numberPattern = Pattern.compile("-?[0-9]+(\\\\.[0-9]+)?");
    private static Pattern integerPattern = Pattern.compile("^[-\\+]?[\\d]*$");
    private static Pattern datePattern = Pattern
            .compile("^([1-9]\\d{3}-)(([0]{0,1}[1-9]-)|([1][0-2]-))(([0-3]{0,1}[0-9]))$");
    private static Pattern datetimePattern = Pattern.compile(
            "((([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29))\\\\s+([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\n");

    private DorisNumberUtils() {
    }

    public static boolean isNumber(String str) {
        Matcher m = numberPattern.matcher(str);
        return m.matches();
    }

    public static boolean isInteger(String str) {
        Matcher m = integerPattern.matcher(str);
        return m.matches();
    }

    public static boolean isDate(String str) {
        Matcher m = datePattern.matcher(str);
        return m.matches();
    }

    public static boolean isDatetime(String str) {
        Matcher m = datetimePattern.matcher(str);
        return m.matches();
    }

    public static String timestampToDateText(long ts) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(ts);
    }

    public static String timestampToDatetimeText(long ts) {
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return datetimeFormat.format(ts);
    }

    public static String dateTextToDatetimeText(String date) {
        // '2021-03-12' -> '2021-03-12 00:00:00'
        return date + " 00:00:00";
    }

    public static String datetimeTextToDateText(String datetime) {
        // '2021-03-12 00:00:00' -> '2021-03-12'
        return datetime.substring(0, 10);
    }

    public static boolean datetimeEqual(String dt1, String dt2) {
        String datetime1 = dt1;
        String datetime2 = dt2;
        if (isDate(dt1)) {
            datetime1 = dateTextToDatetimeText(dt1);
        }
        if (isDate(dt2)) {
            datetime2 = dateTextToDatetimeText(dt2);
        }
        return datetime1.contentEquals(datetime2);
    }

    public static boolean dateEqual(String d1, String d2) {
        String date1 = d1;
        String date2 = d2;
        if (isDatetime(d1)) {
            date1 = datetimeTextToDateText(d1);
        }
        if (isDatetime(d2)) {
            date2 = datetimeTextToDateText(d2);
        }
        return date1.contentEquals(date2);
    }

    public static boolean dateLessThan(String d1, String d2) {
        String date1 = d1;
        String date2 = d2;
        if (isDatetime(d1)) {
            date1 = datetimeTextToDateText(d1);
        }
        if (isDatetime(d2)) {
            date2 = datetimeTextToDateText(d2);
        }
        return date1.compareTo(date2) < 0;
    }

    public static boolean datetimeLessThan(String dt1, String dt2) {
        String datetime1 = dt1;
        String datetime2 = dt2;
        if (isDate(dt1)) {
            datetime1 = dateTextToDatetimeText(dt1);
        }
        if (isDate(dt2)) {
            datetime2 = dateTextToDatetimeText(dt2);
        }
        return datetime1.compareTo(datetime2) < 0;
    }

    public static String getCurrentTimeText() {
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return datetimeFormat.format(new Date());
    }
}
