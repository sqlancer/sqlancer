package sqlancer.transformations;

import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Round double values which are longer than a certain length. e.g. 2.4782565267 -> 2.478.
 *
 * This transformation is not based on JSQLParser.
 */
public class RoundDoubleConstant extends Transformation {

    private Set<String> doubleValueCollector;

    private String currentString;

    private static final int ROUND_LENGTH = 3;
    private DecimalFormat decimalFormat;

    public RoundDoubleConstant() {
        super("round double constant values");
    }

    @Override
    public boolean init(String sql) {
        super.init(sql);
        decimalFormat = new DecimalFormat("#." + "#".repeat(ROUND_LENGTH));

        currentString = sql;
        doubleValueCollector = new HashSet<>();

        String regex = "\\b-?\\d+\\.\\d+\\b";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sql);

        while (matcher.find()) {
            String matchedText = matcher.group();
            String decimalPart = matchedText.replaceAll("\\d+\\.", "");
            int decimalPlaces = decimalPart.length();
            if (decimalPlaces > ROUND_LENGTH) {
                doubleValueCollector.add(matchedText);
            }
        }
        return true;
    }

    @Override
    public void apply() {
        for (String doubleValue : doubleValueCollector) {

            double targetNumber = Double.parseDouble(doubleValue);
            String roundedNumberStr = decimalFormat.format(targetNumber);

            String replacement = currentString.replace(doubleValue, roundedNumberStr);
            String original = currentString;

            tryReplace(null, original, replacement, (p, r) -> {
                currentString = r;
            });
        }
        super.apply();
    }

    @Override
    protected void onStatementChanged() {
        if (statementChangedHandler != null) {
            statementChangedHandler.accept(currentString);
        }
    }
}
