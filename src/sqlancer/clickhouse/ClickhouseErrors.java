package sqlancer.clickhouse;

import java.util.Set;

public class ClickhouseErrors {

    private ClickhouseErrors() {
    }

    public static void addExpressionErrors(Set<String> errors) {
        // errors.add("Illegal type (String) of argument of function not");
        // errors.add("Illegal type String of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        // errors.add("Illegal type Int32 of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        // errors.add("Illegal type UInt32 of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        // errors.add("Illegal type Int32 of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of
        // them.");
        // errors.add("Illegal type String of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of
        // them.");
        // errors.add("Illegal type Int64 of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        errors.add("Illegal type");
        errors.add("Argument at index 1 for function like must be constant");
        errors.add("Argument at index 1 for function notLike must be constant");

        // regex
        errors.add("invalid escape sequence");
        errors.add("invalid character class range");
    }

}
