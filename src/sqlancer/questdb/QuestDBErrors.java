package sqlancer.questdb;

import sqlancer.common.query.ExpectedErrors;

public final class QuestDBErrors {

    private QuestDBErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        // TODO (anxing)
        errors.add("unexpected argument for function: ");
        errors.add("unexpected token:"); // SELECT FROM multiple tables without WHERE/ JOIN clause
        errors.add("boolean expression expected");
        errors.add("Column name expected");
        errors.add("too few arguments for 'in'");
        errors.add("cannot compare TIMESTAMP with type"); // WHERE column IN with nonTIMESTAMP arg
        errors.add("constant expected");
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
