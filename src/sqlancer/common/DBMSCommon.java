package sqlancer.common;

import java.util.ArrayList;
import java.util.List;
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

    public static int getMaxIndexInDoubleArray(double... doubleArray) {
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

    public static boolean areQueryPlanSequencesSimilar(List<String> list1, List<String> list2) {
        return editDistance(list1, list2) <= 1;
    }

    public static int editDistance(List<String> list1, List<String> list2) {
        int[][] dp = new int[list1.size() + 1][list2.size() + 1];
        for (int i = 0; i <= list1.size(); i++) {
            for (int j = 0; j <= list2.size(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    dp[i][j] = Math.min(dp[i - 1][j - 1] + costOfSubstitution(list1.get(i - 1), list2.get(j - 1)),
                            Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
                }
            }
        }
        return dp[list1.size()][list2.size()];
    }

    private static int costOfSubstitution(String string, String string2) {
        return string.equals(string2) ? 0 : 1;
    }

    public static List<String> getCommonFetchErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
        errors.add("but it cannot be referenced from this part of the query");
        errors.add("missing FROM-clause entry for table");

        errors.add("non-integer constant in GROUP BY");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY position");

        errors.add("canceling statement due to statement timeout");

        return errors;
    }

    public static List<String> getCommonExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("You might need to add explicit type casts");
        errors.add("invalid regular expression");
        errors.add("could not determine which collation to use");
        errors.add("invalid regular expression");
        errors.add("operator does not exist");
        errors.add("quantifier operand invalid");
        errors.add("collation mismatch");
        errors.add("collations are not supported");
        errors.add("operator is not unique");
        errors.add("is not a valid binary digit");
        errors.add("invalid hexadecimal digit");
        errors.add("invalid hexadecimal data: odd number of digits");
        errors.add("zero raised to a negative power is undefined");
        errors.add("division by zero");
        errors.add("invalid input syntax for type money");
        errors.add("invalid input syntax for type");
        errors.add("cannot cast type");
        errors.add("value overflows numeric format");
        errors.add("LIKE pattern must not end with escape character");
        errors.add("is of type boolean but expression is of type text");
        errors.add("a negative number raised to a non-integer power yields a complex result");
        errors.add("could not determine polymorphic type because input has type unknown");
        errors.add("character number must be positive");

        errors.addAll(getToCharFunctionErrors());
        errors.addAll(getBitStringOperationErrors());

        return errors;
    }

    public static List<String> getToCharFunctionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("multiple decimal points");
        errors.add("and decimal point together");
        errors.add("multiple decimal points");
        errors.add("cannot use \"S\" twice");
        errors.add("must be ahead of \"PR\"");
        errors.add("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
        errors.add("cannot use \"S\" and \"SG\" together");
        errors.add("cannot use \"S\" and \"MI\" together");
        errors.add("cannot use \"S\" and \"PL\" together");
        errors.add("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
        errors.add("is not a number");

        return errors;
    }

    public static List<String> getBitStringOperationErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("cannot XOR bit strings of different sizes");
        errors.add("cannot AND bit strings of different sizes");
        errors.add("cannot OR bit strings of different sizes");
        errors.add("must be type boolean, not type text");

        return errors;
    }

    public static List<String> getFunctionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("out of valid range"); // get_bit/get_byte
        errors.add("cannot take logarithm of a negative number");
        errors.add("cannot take logarithm of zero");
        errors.add("requested character too large for encoding"); // chr
        errors.add("null character not permitted"); // chr
        errors.add("requested character not valid for encoding"); // chr
        errors.add("requested length too large"); // repeat
        errors.add("invalid memory alloc request size"); // repeat
        errors.add("negative substring length not allowed"); // substr
        errors.add("invalid mask length"); // set_masklen

        return errors;
    }
}
