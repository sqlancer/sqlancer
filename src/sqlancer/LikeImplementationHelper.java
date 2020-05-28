package sqlancer;

public final class LikeImplementationHelper {

    private LikeImplementationHelper() {
    }

    public static boolean match(String str, String regex, int regexPosition, int strPosition, boolean caseSensitive) {
        if (strPosition == str.length() && regexPosition == regex.length()) {
            return true;
        }
        if (regexPosition >= regex.length()) {
            return false;
        }
        char cur = regex.charAt(regexPosition);
        if (strPosition >= str.length()) {
            if (cur == '%') {
                return match(str, regex, regexPosition + 1, strPosition, caseSensitive);
            } else {
                return false;
            }
        }
        switch (cur) {
        case '%':
            // match
            boolean foundMatch = match(str, regex, regexPosition, strPosition + 1, caseSensitive);
            if (!foundMatch) {
                return match(str, regex, regexPosition + 1, strPosition, caseSensitive);
            } else {
                return true;
            }
        case '_':
            return match(str, regex, regexPosition + 1, strPosition + 1, caseSensitive);
        default:
            boolean charMatches;
            if (!caseSensitive) {
                charMatches = toUpper(cur) == toUpper(str.charAt(strPosition));
            } else {
                charMatches = cur == str.charAt(strPosition);
            }
            if (charMatches) {
                return match(str, regex, regexPosition + 1, strPosition + 1, caseSensitive);
            } else {
                return false;
            }
        }
    }

    private static char toUpper(char cur) {
        if (cur >= 'a' && cur <= 'z') {
            return (char) (cur + 'A' - 'a');
        } else {
            return cur;
        }
    }

}
