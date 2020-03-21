package sqlancer.cockroachdb;

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
		
		if (true) {
			errors.add("aggregate functions are not allowed in GROUP BY");
			errors.add(" must appear in the GROUP BY clause or be used in an aggregate function"); // TODO: better control what is generated in a view
		}
		
		if (true) {
			// TODO https://github.com/cockroachdb/cockroach/issues/44757
			errors.add("no builtin aggregate");
		}
		
		errors.add("unable to vectorize execution plan"); // SET vectorize=experimental_always;
		errors.add(" mismatched physical types at index"); // SET vectorize=experimental_always;
		errors.add("unsupported type time");
		errors.add("unsupported type varbit");
		errors.add("unsupported type bit");
		
		errors.add("unknown signature: xor_agg(string)");
		errors.add("arguments to xor must all be the same length");
	}

	private static void addIntervalTypeErrors(Set<String> errors) {
		errors.add("overflow during Encode");
		errors.add("as type interval");
	}

	private static void addJoinTypes(Set<String> errors) {
		errors.add("JOIN/USING types");
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
		errors.add("to_ip(): invalid IP format"); // to_ip
		errors.add("invalid IP format"); // to_ip
		errors.add("incorrect UUID length"); // to_uuid
		errors.add("incorrect UUID format"); // to_uuid
		errors.add("substring(): negative substring length"); // substring
		errors.add("negative substring length"); // substring
		errors.add("must be greater than zero"); // split_part
	}

	public static void addTransactionErrors(Set<String> errors) {
		errors.add("current transaction is aborted");
	}

}
