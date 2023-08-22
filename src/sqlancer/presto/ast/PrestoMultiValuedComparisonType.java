package sqlancer.presto.ast;

import sqlancer.Randomly;

public enum PrestoMultiValuedComparisonType {
    ANY, SOME, ALL;

    public static PrestoMultiValuedComparisonType getRandom() {
        return Randomly.fromOptions(values());
    }
}
