package sqlancer.voltdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class VoltDBFunction extends NewFunctionNode<VoltDBExpression, VoltDBFunction.VoltDBFunctionName>
        implements VoltDBExpression {

    // Functions the generator calls; the name is printed as written.
    public enum VoltDBFunctionName {
        // numeric
        ABS, FLOOR, CEILING, MOD, CHAR_LENGTH,
        // string
        SUBSTRING, UPPER, LOWER, TRIM, LTRIM, RTRIM,
        // any type
        COALESCE,
        // FLOAT math (milestone 19): FLOAT results
        SQRT, EXP, LN, LOG10, POWER, SIN, COS,
        // parts of a DATE or TIMESTAMP (milestone 19): integer results
        YEAR, MONTH, DAY, HOUR, MINUTE, DAYOFWEEK, DAYOFYEAR, QUARTER, WEEK, WEEKDAY,
        // SINCE_EPOCH(unit, TIMESTAMP): BIGINT; TO_TIMESTAMP(unit, integer): TIMESTAMP; DATEADD(unit, n, t) and
        // TRUNCATE(unit, t): t's type (milestone 19)
        SINCE_EPOCH, TO_TIMESTAMP, DATEADD, TRUNCATE,
        // JSON in VARCHAR (milestone 19): FIELD(doc, name), ARRAY_ELEMENT(array, i) and SET_FIELD(doc, name, value)
        // return VARCHAR, ARRAY_LENGTH(array) an integer
        FIELD, ARRAY_ELEMENT, SET_FIELD, ARRAY_LENGTH;

        // whether the PQS evaluator models the function; the milestone 19 functions it does not
        public boolean isModelled() {
            return ordinal() <= COALESCE.ordinal();
        }
    }

    public VoltDBFunction(List<VoltDBExpression> args, VoltDBFunctionName func) {
        super(args, func);
    }

}
