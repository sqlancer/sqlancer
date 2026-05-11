package sqlancer.questdb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class QuestDBErrors {

    private QuestDBErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // QuestDB-specific expression errors
        errors.add("unexpected argument for function: ");
        errors.add("unexpected token:"); // SELECT FROM multiple tables without WHERE/ JOIN clause
        errors.add("boolean expression expected");
        errors.add("Column name expected");
        errors.add("too few arguments for 'in'");
        errors.add("cannot compare TIMESTAMP with type"); // WHERE column IN with nonTIMESTAMP arg
        errors.add("constant expected");
        errors.add("invalid column reference");
        errors.add("syntax error");
        errors.add("unexpected end of statement");
        errors.add("invalid operator");
        errors.add("type mismatch");
        errors.add("division by zero");
        errors.add("invalid function call");
        errors.add("missing FROM clause");
        errors.add("duplicate column name");

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    public static List<String> getGroupByErrors() {
        ArrayList<String> errors = new ArrayList<>();
        
        // QuestDB-specific GROUP BY errors
        errors.add("GROUP BY expression must be in SELECT list");
        errors.add("aggregate function not allowed in GROUP BY");
        errors.add("column must appear in GROUP BY clause");
        
        return errors;
    }

    public static void addGroupByErrors(ExpectedErrors errors) {
        errors.addAll(getGroupByErrors());
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // QuestDB-specific insert errors
        errors.add("Invalid column");
        errors.add("inconvertible types:");
        errors.add("inconvertible value:");
        errors.add("column count mismatch");
        errors.add("duplicate key value");
        errors.add("constraint violation");
        errors.add("invalid data type");
        errors.add("value too large");
        errors.add("missing required column");
        errors.add("table does not exist");
        errors.add("permission denied");
        errors.add("syntax error in INSERT statement");

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }
    
    public static void addAllErrors(ExpectedErrors errors) {
        addExpressionErrors(errors);
        addGroupByErrors(errors);
        addInsertErrors(errors);
    }
}
