package sqlancer.presto;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class PrestoConstantUtils {

    private PrestoConstantUtils() {
    }

    public static String removeNoneAscii(String str) {
        return str.replaceAll("[^\\x00-\\x7F]", "");
    }

    public static String removeNonePrintable(String str) { // All Control Char
        return str.replaceAll("[\\p{C}]", "");
    }

    public static String removeOthersControlChar(String str) { // Some Control Char
        return str.replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]", "");
    }

    public static String removeAllControlChars(String str) {
        return removeOthersControlChar(removeNonePrintable(str)).replaceAll("[\\r\\n\\t]", "");
    }

    public static BigDecimal getDecimal(double val, int scale, int precision) {
        int part = precision - scale;
        // long part
        long lng = (long) val;
        // decimal places
        double d1 = val - lng;
        String xStr = Long.toString(lng);
        String substring = xStr.substring(xStr.length() - part);
        long newX = substring.isEmpty() ? 0 : Long.parseLong(substring);
        double finalD = newX + d1;
        return new BigDecimal(finalD).setScale(scale, RoundingMode.CEILING);
    }

}
