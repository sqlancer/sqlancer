package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLSequenceGenerator {

    private YSQLSequenceGenerator() {
    }

    public static SQLQueryAdapter createSequence(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("TEMPORARY", "TEMP"));
        }
        sb.append(" SEQUENCE");
        // TODO keep track of sequences
        sb.append(" IF NOT EXISTS");
        // TODO generate sequence names
        sb.append(" seq");
        
        String dataType = null;
        long typeMinValue = Integer.MIN_VALUE;
        long typeMaxValue = Integer.MAX_VALUE;
        
        if (Randomly.getBoolean()) {
            dataType = Randomly.fromOptions("smallint", "integer", "bigint");
            sb.append(" AS ");
            sb.append(dataType);
            
            // Set type limits
            switch (dataType) {
                case "smallint":
                    typeMinValue = -32768;
                    typeMaxValue = 32767;
                    break;
                case "integer":
                    typeMinValue = Integer.MIN_VALUE;
                    typeMaxValue = Integer.MAX_VALUE;
                    break;
                case "bigint":
                    typeMinValue = Long.MIN_VALUE;
                    typeMaxValue = Long.MAX_VALUE;
                    break;
            }
        }
        
        // Track min/max values to generate valid sequences
        Long minValue = null;
        Long maxValue = null;
        
        if (Randomly.getBoolean()) {
            sb.append(" INCREMENT");
            if (Randomly.getBoolean()) {
                sb.append(" BY");
            }
            sb.append(" ");
            // Increment can be negative but not zero
            long increment = globalState.getRandomly().getInteger();
            if (increment == 0) increment = 1;
            sb.append(increment);
        }
        
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" MINVALUE");
                sb.append(" ");
                // Generate minValue within type limits
                long range = typeMaxValue - typeMinValue;
                if (range > 0 && range <= Integer.MAX_VALUE) {
                    minValue = typeMinValue + Math.abs(globalState.getRandomly().getInteger()) % range;
                } else {
                    minValue = typeMinValue + Math.abs(globalState.getRandomly().getInteger());
                }
                // Ensure minValue is within type bounds
                minValue = Math.max(typeMinValue, Math.min(minValue, typeMaxValue - 1));
                sb.append(minValue);
            } else {
                sb.append(" NO MINVALUE");
            }
        }
        
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" MAXVALUE");
                sb.append(" ");
                if (minValue != null) {
                    // Ensure maxValue > minValue and within type limits
                    long remainingRange = typeMaxValue - minValue;
                    if (remainingRange > 1) {
                        maxValue = minValue + 1 + Math.abs(globalState.getRandomly().getInteger()) % Math.min(remainingRange - 1, Integer.MAX_VALUE);
                    } else {
                        maxValue = typeMaxValue;
                    }
                } else {
                    // Generate maxValue within type limits
                    long range = typeMaxValue - typeMinValue;
                    if (range > 0 && range <= Integer.MAX_VALUE) {
                        maxValue = typeMinValue + Math.abs(globalState.getRandomly().getInteger()) % range;
                    } else {
                        maxValue = (long) globalState.getRandomly().getInteger();
                    }
                }
                // Ensure maxValue is within type bounds
                maxValue = Math.max(typeMinValue + 1, Math.min(maxValue, typeMaxValue));
                sb.append(maxValue);
            } else {
                sb.append(" NO MAXVALUE");
            }
            errors.add("must be less than MAXVALUE");
        }
        
        if (Randomly.getBoolean()) {
            sb.append(" START");
            if (Randomly.getBoolean()) {
                sb.append(" WITH");
            }
            sb.append(" ");
            long startValue;
            if (minValue != null && maxValue != null) {
                // Generate start value between min and max
                long range = maxValue - minValue;
                if (range > 0) {
                    startValue = minValue + Math.abs(globalState.getRandomly().getInteger()) % Math.min(range, Integer.MAX_VALUE);
                } else {
                    startValue = minValue;
                }
            } else if (minValue != null) {
                // Start value should be >= minValue and within type bounds
                long range = typeMaxValue - minValue;
                if (range > 0) {
                    startValue = minValue + Math.abs(globalState.getRandomly().getInteger()) % Math.min(range, Integer.MAX_VALUE);
                } else {
                    startValue = minValue;
                }
            } else if (maxValue != null) {
                // Start value should be <= maxValue and within type bounds
                long range = maxValue - typeMinValue;
                if (range > 0) {
                    startValue = typeMinValue + Math.abs(globalState.getRandomly().getInteger()) % Math.min(range, Integer.MAX_VALUE);
                } else {
                    startValue = maxValue;
                }
            } else {
                // Generate within type bounds
                long range = typeMaxValue - typeMinValue;
                if (range > 0 && range <= Integer.MAX_VALUE) {
                    startValue = typeMinValue + Math.abs(globalState.getRandomly().getInteger()) % range;
                } else {
                    startValue = globalState.getRandomly().getInteger();
                }
            }
            // Ensure start value is within all bounds
            if (minValue != null) {
                startValue = Math.max(startValue, minValue);
            }
            if (maxValue != null) {
                startValue = Math.min(startValue, maxValue);
            }
            startValue = Math.max(typeMinValue, Math.min(startValue, typeMaxValue));
            sb.append(startValue);
            errors.add("cannot be less than MINVALUE");
            errors.add("cannot be greater than MAXVALUE");
        }
        
        if (Randomly.getBoolean()) {
            sb.append(" CACHE ");
            // Use reasonable cache values (1-10000)
            sb.append(globalState.getRandomly().getInteger(1, 10000));
        }
        errors.add("is out of range");
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" NO");
            }
            sb.append(" CYCLE");
        }
        if (Randomly.getBoolean()) {
            sb.append(" OWNED BY ");
            // if (Randomly.getBoolean()) {
            sb.append("NONE");
            // } else {
            // sb.append(s.getRandomTable().getRandomColumn().getFullQualifiedName());
            // }
        }
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonFetchErrors(errors);
        YSQLErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
