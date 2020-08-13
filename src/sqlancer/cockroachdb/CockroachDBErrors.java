package sqlancer.cockroachdb;

import sqlancer.common.query.ExpectedErrors;

public final class CockroachDBErrors {

    private CockroachDBErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add(" non-streaming operator encountered when vectorize=auto");

        if (CockroachDBBugs.bug46915) {
            errors.add("ERROR: at or near \"unknown\": syntax error");
        }

        if (CockroachDBBugs.bug45703) {
            errors.add("github.com/cockroachdb/cockroach/pkg/sql/execinfra/expr.go:78: processExpression()");
        }

        errors.add("exceeds supported timestamp bounds");

        errors.add("cannot cast negative integer to bit varying with unbounded width");

        errors.add("negative value for LIMIT");
        errors.add("negative value for OFFSET");

        errors.add("LIKE regexp compilation failed");
        errors.add("error parsing regexp");

        errors.add("expected 9223372036854775808 to be of type int, found type decimal"); // int overflow
        errors.add("integer out of range");
        errors.add("expected -9223372036854775809 to be of type int, found type decimal");
        errors.add("to be of type int4, found type decimal");

        errors.add("as type bool: invalid bool value");
        errors.add("as type int: strconv.ParseInt");
        errors.add("as type float: strconv.ParseFloat: parsing");

        errors.add("is not in select list");
        errors.add("non-integer constant in ORDER BY");

        errors.add("collatedstring");

        errors.add("invalid cast:");

        // string to bytes
        errors.add("invalid bytea escape sequence");
        errors.add("bytea encoded value ends with incomplete escape sequence");
        errors.add("bytea encoded value ends with escape character");
        errors.add("encoding/hex: invalid byte");

        errors.add("unsupported comparison operator: <bytes> > <string>");
        errors.add("unsupported comparison operator: <bytes> < <string>");
        errors.add("unsupported comparison operator: <string> > <bytes>");
        errors.add("unsupported comparison operator: <bytes> >= <string>");
        errors.add("unsupported comparison operator: <string> < <bytes>");
        errors.add("ERROR: unsupported comparison operator: <string> != <bytes>");
        errors.add("unsupported comparison operator: <string> IS NOT DISTINCT FROM <bytes>");
        errors.add("unsupported comparison operator: <bytes> IS NOT DISTINCT FROM <string>");
        errors.add("unsupported comparison operator: <string> IS DISTINCT FROM <bytes>");
        errors.add("unsupported comparison operator: <bytes> = <string>");
        errors.add("unsupported comparison operator: <string> >= <bytes>");
        errors.add("ERROR: unsupported comparison operator: <string> = <bytes>");
        errors.add("unsupported comparison operator: <bytes> IS DISTINCT FROM <string>");
        errors.add("unsupported comparison operator: <bytes> <= <string>");
        errors.add("unsupported comparison operator: <bytes> != <string>");
        errors.add("unsupported comparison operator: <string> = <bytes>");
        errors.add("unsupported comparison operator: <string> <= <bytes>");
        errors.add("to be of type string, found type bytes");
        errors.add("unknown signature: left(string, int) (desired <bytes>)");
        errors.add("unknown signature: bit_length(collatedstring");
        errors.add("ERROR: unknown signature: left(collatedstring");
        errors.add("unsupported comparison operator: <string> !~ <collatedstring{");
        errors.add("unsupported comparison operator: <collatedstring");
        errors.add(" unsupported comparison operator: <string> NOT LIKE <collatedstring{");
        errors.add("unsupported comparison operator: <string> != <bytes>");
        errors.add("expected DEFAULT expression to have type bytes");
        errors.add("value type string doesn't match type bytes of column");
        errors.add("as decimal, found type: int");
        errors.add("to be of type decimal, found type float");
        errors.add("to be of type float, found type decimal");
        errors.add("to be of type bytes, found type string");
        errors.add("as bytes, found type: string");

        errors.add("bit string length"); // TODO restrict generated bit constants
        errors.add("could not parse string as bit array");

        errors.add("ambiguous call");

        errors.add(" could not produce a query plan conforming to the");
        errors.add("LOOKUP can only be used with INNER or LEFT joins"); // TODO

        errors.add("ambiguous binary operator: <unknown> || <unknown>");
        errors.add(" ERROR: unsupported binary operator: <string> || <string> (desired <bytes>)");
        errors.add("unsupported binary operator: <unknown> || <string> (desired <bytes>)");
        errors.add("incompatible value type: unsupported binary operator: <string> || <unknown> (desired <bytes>)");
        errors.add("unsupported binary operator: <string> || <unknown> (desired <bytes>)");
        errors.add("unsupported binary operator: <string> || <string> (desired <bytes>)");
        errors.add("parsing as type timestamp: empty or blank input");
        errors.add("parsing as type timestamp: field");
        errors.add("as type time");
        errors.add("as TimeTZ");
        errors.add("as type decimal");
        addIntervalTypeErrors(errors);
        addFunctionErrors(errors);
        addGroupByErrors(errors);
        addJoinTypes(errors);
        errors.add("as int4, found type: decimal");
        errors.add("to be of type int2, found type decimal");
        errors.add("to be of type int, found type decimal"); // arithmetic overflows
        errors.add("unknown signature: left(string, decimal)");
        errors.add("unknown signature: left(bytes, decimal) (desired <bytes>)");
        errors.add("numeric constant out of int64 range");
        errors.add("unknown signature: overlay(string, string, decimal)");
        errors.add("unknown signature: substring(string, int, decimal)");
        errors.add("unsupported binary operator: <unknown> + <decimal> (desired <int>)");
        errors.add("unsupported comparison operator");
        errors.add("unknown signature: chr(decimal) (desired <string>)");
        errors.add("unknown signature: to_english(decimal) (desired <string>)");
        errors.add("unknown signature: to_hex(decimal) (desired <string>)");
        errors.add("incompatible value type: expected rowid to be of type decimal, found type int");
        errors.add("unknown signature: to_english(decimal)");
        errors.add("unknown signature: chr(decimal)");
        errors.add(" unknown signature: left(string, int2) (desired <bytes>)");
        errors.add("unknown signature: split_part(string, string, decimal) (desired <string>)");
        errors.add(" unknown signature: substring(string, ");
        errors.add("division by zero");
        errors.add("as int, found type: decimal");
        errors.add("value type decimal doesn't match type int2 ");
        errors.add("has type decimal");
        errors.add("to be of type decimal, found type int");
        errors.add("value type decimal doesn't match type int");
        errors.add("unknown signature: substring(string, decimal, int)");
        errors.add("unsupported binary operator: <int> / <int> (desired <int4>)");
        errors.add("(desired <int>)");
        errors.add("(desired <int2>)");
        errors.add("(desired <int4>)");
        errors.add("found type: decimal");
        errors.add("(desired <decimal>)");
        errors.add("unknown signature: to_hex(decimal)");
        errors.add("unknown signature: split_part(string, string, decimal)");
        errors.add("unknown signature: left(bytes, decimal)");
        errors.add("division undefined");
        errors.add("decimal out of range");
        errors.add("unknown signature: xor_agg(decimal)");
        errors.add("unknown signature: sum_int(decimal)");
        errors.add("unknown signature: bit_and(decimal)");
        errors.add("unknown signature: bit_or(decimal)");

        errors.add("exists but is not a directory"); // TODO

        errors.add("could not parse JSON: trailing characters after JSON document");
        errors.add("could not parse JSON: unable to decode JSON: invalid character");
        errors.add("could not parse JSON: unable to decode JSON: EOF");
        errors.add("could not parse JSON: unable to decode JSON: unexpected EOF");
        errors.add("can't order by column type jsonb");

        // TODO: better control what is generated in a view
        errors.add("aggregate functions are not allowed in GROUP BY");
        errors.add(" must appear in the GROUP BY clause or be used in an aggregate function");

        if (CockroachDBBugs.bug44757) {
            errors.add("no builtin aggregate");
        }

        errors.add("unable to vectorize execution plan"); // SET vectorize=experimental_always;
        errors.add(" mismatched physical types at index"); // SET vectorize=experimental_always;
        errors.add("unsupported type time");
        errors.add("unsupported type varbit");
        errors.add("unsupported type bit");

        errors.add("unknown signature: xor_agg(string)");
        errors.add("unknown signature: string_agg(string, bytes)");
        errors.add("unknown signature: string_agg(bytes, string)");
        errors.add("arguments to xor must all be the same length");
        errors.add("unknown signature: acos(decimal)");

        errors.add("argument of OFFSET must be type int, not type decimal");
        errors.add("ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list");

        addArrayErrors(errors);
    }

    private static void addArrayErrors(ExpectedErrors errors) {
        // arrays
        errors.add("cannot determine type of empty array");
        errors.add("unknown signature: max(unknown[])");
        errors.add("unknown signature: min(unknown[])");
        errors.add("unknown signature: max(interval[])");
        errors.add("unknown signature: min(interval[])");
        errors.add("unknown signature: max(string[])");
        errors.add("unknown signature: min(string[])");
        errors.add("unknown signature: max(decimal[])");
        errors.add("unknown signature: min(decimal[])");
        errors.add("unknown signature: max(varbit[])");
        errors.add("unknown signature: min(varbit[])");
        errors.add("unknown signature: max(int[])");
        errors.add("unknown signature: min(int[])");
        errors.add("unknown signature: min(bool[])");
        errors.add("unknown signature: max(bool[])");
        errors.add("unknown signature: max(timestamp[])");
        errors.add("unknown signature: min(timestamp[])");
        errors.add("unknown signature: min(timestamptz[])");
        errors.add("unknown signature: max(timestamptz[])");
        errors.add("unknown signature: min(timetz[])");
        errors.add("unknown signature: max(timetz[])");
        errors.add("unknown signature: max(time[])");
        errors.add("unknown signature: min(time[])");
        errors.add("unknown signature: min(int2[])");
        errors.add("unknown signature: max(int2[])");
        errors.add("unknown signature: max(int4[])");
        errors.add("unknown signature: min(int4[])");
        errors.add("unknown signature: max(bytes[])");
        errors.add("unknown signature: min(bytes[])");
        errors.add("unknown signature: min(bit[])");
        errors.add("unknown signature: max(bit[])");
        errors.add("unknown signature: min(float[])");
        errors.add("unknown signature: max(float[])");

        errors.add("array must be enclosed in { and }"); // when casting a string to an array
        errors.add("extra text after closing right brace");
        errors.add("unimplemented: nested arrays not supported"); // e.g., casting a string {{1}} to an array
        errors.add("malformed array");

        errors.add("https://github.com/cockroachdb/cockroach/issues/35707"); // arrays don't support ORDER BY

        errors.add("as bytes[], found type: varbit[]");
        errors.add("to be of type decimal[], found type float[]");
        errors.add("to be of type int[], found type decimal[]");

        errors.add("to be of type unknown[]"); // IF with null array
    }

    private static void addIntervalTypeErrors(ExpectedErrors errors) {
        errors.add("overflow during Encode");
        errors.add("as type interval");
    }

    private static void addJoinTypes(ExpectedErrors errors) {
        errors.add("JOIN/USING types");
    }

    private static void addGroupByErrors(ExpectedErrors errors) {
        errors.add("non-integer constant in GROUP BY");

        // https://github.com/cockroachdb/cockroach/pull/46649 -> aggregates on NULL are
        // not typed strings
        errors.add("ERROR: argument of HAVING must be type bool, not type string"); // MAX(NULL) etc.
        errors.add("incompatible condition type: string"); // CASE WHEN MAX(NULL) etc.
        errors.add("incompatible NOT argument type: string"); // NOT MAX(NULL) etc.
        errors.add("incompatible AND argument type: string");
        errors.add("incompatible OR argument type: string");
        errors.add("ERROR: incompatible IF condition type: string");
        errors.add(", found type: string");
        errors.add("incompatible NULLIF expressions");
        errors.add("ERROR: unsupported binary operator");
        errors.add("incompatible value type");
        errors.add(" ERROR: incompatible IF expressions");
        errors.add(" to be of type string");
        errors.add("found type string");

        errors.add("unknown signature: abs(string)");
        errors.add("unknown signature: acos(string)");

    }

    private static void addFunctionErrors(ExpectedErrors errors) {
        // functions
        errors.add("abs of min integer value (-9223372036854775808) not defined"); // ABS
        errors.add("the input string must not be empty"); // ASCII
        errors.add("unknown signature: substring(string, decimal)"); // overflow
        errors.add("overlay(): non-positive substring length not allowed"); // overlay
        errors.add("non-positive substring length not allowed"); // overlay
        errors.add("lpad(): requested length too large"); // lpad
        errors.add("input value must be >= 0"); // chr
        errors.add("input value must be <= 1114111 (maximum Unicode code point)"); // chr
        errors.add("to_ip(): invalid IP format"); // to_ip
        errors.add("invalid IP format"); // to_ip
        errors.add("incorrect UUID length"); // to_uuid
        errors.add("incorrect UUID format"); // to_uuid
        errors.add("substring(): negative substring length"); // substring
        errors.add("negative substring length"); // substring
        errors.add("must be greater than zero"); // split_part
    }

    public static void addTransactionErrors(ExpectedErrors errors) {
        errors.add("current transaction is aborted");
    }

}
