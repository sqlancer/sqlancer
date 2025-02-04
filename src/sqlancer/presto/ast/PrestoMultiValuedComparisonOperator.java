package sqlancer.presto.ast;

import sqlancer.Randomly;
import sqlancer.presto.PrestoSchema;

public enum PrestoMultiValuedComparisonOperator {
    EQUALS("="), NOT_EQUALS("<>"), NOT_EQUALS_ALT("!="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"),
    SMALLER_EQUALS("<=");

    private final String stringRepresentation;

    PrestoMultiValuedComparisonOperator(String stringRepresentation) {
        this.stringRepresentation = stringRepresentation;
    }

    public static PrestoMultiValuedComparisonOperator getRandom() {
        return Randomly.fromOptions(values());
    }

    public static PrestoMultiValuedComparisonOperator getRandomForType(PrestoSchema.PrestoCompositeDataType type) {
        PrestoSchema.PrestoDataType dataType = type.getPrimitiveDataType();

        switch (dataType) {
        case BOOLEAN:
        case INT:
        case FLOAT:
        case DECIMAL:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case TIME_WITH_TIME_ZONE:
        case TIMESTAMP_WITH_TIME_ZONE:
            return getRandom();
        default:
            return Randomly.fromOptions(EQUALS, NOT_EQUALS, NOT_EQUALS_ALT);
        }
    }

    public String getStringRepresentation() {
        return stringRepresentation;
    }

}
