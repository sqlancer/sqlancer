package sqlancer.yugabyte.ycql;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class YCQLErrors {

    private YCQLErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("Signature mismatch in call to builtin function");
        errors.add("Qualified name not allowed for column reference");
        errors.add("Datatype Mismatch");
        errors.add("Invalid Datatype");
        errors.add("Invalid CQL Statement");
        errors.add("Invalid SQL Statement");
        errors.add("Order by clause contains invalid expression");
        errors.add("Invalid Function Call");

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

}
