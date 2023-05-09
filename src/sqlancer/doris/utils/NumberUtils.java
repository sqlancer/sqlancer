package sqlancer.doris.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NumberUtils {
    private static Pattern numberPattern = Pattern.compile("-?[0-9]+(\\\\.[0-9]+)?");
    private static Pattern integerPattern = Pattern.compile("^[-\\+]?[\\d]*$");
    private static Pattern datePattern = Pattern.compile("^([1-9]\\d{3}-)(([0]{0,1}[1-9]-)|([1][0-2]-))(([0-3]{0,1}[0-9]))$");
    private static Pattern datetimePattern = Pattern.compile("((([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29))\\\\s+([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\n");

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
        return dateFormat.format(ts);
    }

    public static String timestampToDatetimeText(long ts) {
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

    public static boolean datetimeEqual(String datetime1, String datetime2) {
        if (isDate(datetime1)) datetime1 = dateTextToDatetimeText(datetime1);
        if (isDate(datetime2)) datetime2 = dateTextToDatetimeText(datetime2);
        return datetime1.contentEquals(datetime2);
    }

    public static boolean dateEqual(String date1, String date2) {
        if (isDatetime(date1)) date1 = datetimeTextToDateText(date1);
        if (isDatetime(date1)) date2 = datetimeTextToDateText(date2);
        return date1.contentEquals(date2);
    }

    public static boolean dateLessThan(String date1, String date2) {
        if (isDatetime(date1)) date1 = datetimeTextToDateText(date1);
        if (isDatetime(date1)) date2 = datetimeTextToDateText(date2);
        return date1.compareTo(date2) < 0;
    }

    public static boolean datetimeLessThan(String datetime1, String datetime2) {
        if (isDate(datetime1)) datetime1 = dateTextToDatetimeText(datetime1);
        if (isDate(datetime2)) datetime2 = dateTextToDatetimeText(datetime2);
        return datetime1.compareTo(datetime2) < 0;
    }

    public static String getCurrentTimeText() {
        return datetimeFormat.format(new Date());
    }
}
