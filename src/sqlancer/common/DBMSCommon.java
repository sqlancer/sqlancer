package sqlancer.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DBMSCommon {

    private static final Pattern SQLANCER_INDEX_PATTERN = Pattern.compile("^i\\d+");

    private DBMSCommon() {
    }

    public static String createTableName(int nr) {
        return String.format("t%d", nr);
    }

    public static String createColumnName(int nr) {
        return String.format("c%d", nr);
    }

    public static String createIndexName(int nr) {
        return String.format("i%d", nr);
    }

    public static boolean matchesIndexName(String indexName) {
        Matcher matcher = SQLANCER_INDEX_PATTERN.matcher(indexName);
        return matcher.matches();
    }

    public static int getMaxIndexInDoubleArrary(double... doubleArray) {
        int maxIndex = 0;
        double maxValue = 0.0;
        for (int j = 0; j < doubleArray.length; j++) {
            double curReward = doubleArray[j];
            if (curReward > maxValue) {
                maxIndex = j;
                maxValue = curReward;
            }
        }
        return maxIndex;
    }
}
