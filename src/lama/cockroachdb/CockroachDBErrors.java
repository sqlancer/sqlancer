package lama.cockroachdb;

import java.util.Set;

public class CockroachDBErrors {
	
	public static void addExpressionErrors(Set<String> errors) {
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
		
		// TODO: do not generate incorrect ASC/DESC ordering terms
		errors.add("ERROR: at or near \"asc\": syntax error");
		errors.add("ERROR: at or near \"desc\": syntax error");
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
		errors.add("to be of type decimal, found type float");
		errors.add("to be of type bytes, found type string");
		errors.add("as bytes, found type: string");
		
		errors.add("bit string length"); // TODO restrict generated bit constants
		errors.add("could not parse string as bit array");
		
		
		errors.add("ambiguous call");
		
		addFunctionErrors(errors);
		addGroupByErrors(errors);
	}

	private static void addGroupByErrors(Set<String> errors) {
		errors.add("non-integer constant in GROUP BY");
	}

	private static void addFunctionErrors(Set<String> errors) {
		// functions
		errors.add("abs of min integer value (-9223372036854775808) not defined"); // ABS
		errors.add("the input string must not be empty"); // ASCII
		errors.add("unknown signature: substring(string, decimal)"); // overflow
		errors.add("overlay(): non-positive substring length not allowed"); // overlay
		errors.add("non-positive substring length not allowed"); // overlay
		errors.add("lpad(): requested length too large"); // lpad
		errors.add("input value must be >= 0"); // chr
		errors.add("input value must be <= 1114111 (maximum Unicode code point)"); // chr
	}

	public static void addTransactionErrors(Set<String> errors) {
		errors.add("current transaction is aborted");
	}

}
