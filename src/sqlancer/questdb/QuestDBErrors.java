package sqlancer.questdb;

import sqlancer.common.query.ExpectedErrors;

public final class QuestDBErrors {

    private QuestDBErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        // TODO (anxing)
    }

    public static void addGroupByErrors(ExpectedErrors errors) {
        // TODO (anxing)
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        // TODO (anxing)
        errors.add("Invalid column");
        errors.add("inconvertible types:");
        errors.add("inconvertible value:");
    }
}
