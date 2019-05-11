package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Expression.Constant;
import lama.sqlite3.gen.QueryGenerator;
import lama.sqlite3.schema.SQLite3DataType;

class TestCastToNumeric {

	@Test
	void testLong() {
		long numbers[] = new long[] { 0, 1, 123, Long.MAX_VALUE, Long.MIN_VALUE };
		for (long number : numbers) {
			assertEquals(castLongConstant(number), number);
		}
	}

	class StringTestTriple {
		String value;
		SQLite3DataType type;
		Object expectedCastValue;

		public StringTestTriple(String value, SQLite3DataType type, Object expectedCastValue) {
			this.value = value;
			this.type = type;
			this.expectedCastValue = expectedCastValue;
		}
	}

	@Test
	void testString() {
		List<StringTestTriple> triples = new ArrayList<>();
		triples.add(new StringTestTriple("", SQLite3DataType.INT, 0L));
		triples.add(new StringTestTriple("a", SQLite3DataType.INT, 0L));
		triples.add(new StringTestTriple("123a", SQLite3DataType.INT, 123L));
		triples.add(new StringTestTriple("3", SQLite3DataType.INT, 3L));
		triples.add(new StringTestTriple("-3", SQLite3DataType.INT, -3L));
		triples.add(new StringTestTriple("-3.0", SQLite3DataType.INT, -3L));
		triples.add(new StringTestTriple("0.0", SQLite3DataType.INT, 0L));
		triples.add(new StringTestTriple("+0", SQLite3DataType.INT, 0L));
		triples.add(new StringTestTriple("+9", SQLite3DataType.INT, 9L));
		triples.add(new StringTestTriple("++9", SQLite3DataType.INT, 0L));
		triples.add(new StringTestTriple("+-9", SQLite3DataType.INT, 0L));
		triples.add(new StringTestTriple("3.0e+5", SQLite3DataType.INT, 300000L));

		triples.add(new StringTestTriple("-3.2", SQLite3DataType.REAL, -3.2d));
		triples.add(new StringTestTriple("10e9", SQLite3DataType.REAL, 10000000000.0));
		triples.add(new StringTestTriple("-0.0", SQLite3DataType.REAL, 0.0d));

		
		
		for (StringTestTriple triple : triples) {
			Constant castVal = QueryGenerator.castToNumeric(Constant.createTextConstant(triple.value));
			assertEquals(triple.value.toString(), triple.expectedCastValue, castVal.getValue());
		}
	}

	@Test
	void testBinary() {
		List<StringTestTriple> triples = new ArrayList<>();
		triples.add(new StringTestTriple("112B3980", SQLite3DataType.INT, 0L)); // +9�
		triples.add(new StringTestTriple("0936", SQLite3DataType.INT, 6L)); // 6
		triples.add(new StringTestTriple("0C36", SQLite3DataType.INT, 6L)); // 6
		triples.add(new StringTestTriple("0a36", SQLite3DataType.INT, 6L)); // 6
		triples.add(new StringTestTriple("0b36", SQLite3DataType.INT, 6L)); // 6
		triples.add(new StringTestTriple("0c36", SQLite3DataType.INT, 6L)); // 6
		triples.add(new StringTestTriple("0d36", SQLite3DataType.INT, 6L)); // 6
		triples.add(new StringTestTriple("0e36", SQLite3DataType.INT, 0L)); // 6
		triples.add(new StringTestTriple("1a347C", SQLite3DataType.INT, 0L)); // 
		triples.add(new StringTestTriple("1b347C", SQLite3DataType.INT, 0L)); // 
		triples.add(new StringTestTriple("1C32", SQLite3DataType.INT, 0L)); // FS2
		triples.add(new StringTestTriple("1D32", SQLite3DataType.INT, 0L)); // GS2
		triples.add(new StringTestTriple("1e32", SQLite3DataType.INT, 0L)); // RS2
		triples.add(new StringTestTriple("1f32", SQLite3DataType.INT, 0L)); // RS2
		triples.add(new StringTestTriple("2032", SQLite3DataType.INT, 2L)); // RS2
		triples.add(new StringTestTriple("09013454", SQLite3DataType.INT, 0L)); // RS2
		triples.add(new StringTestTriple("2016347C", SQLite3DataType.INT, 0L)); // 
		triples.add(new StringTestTriple("2017347C", SQLite3DataType.INT, 0L)); // 
		triples.add(new StringTestTriple("2018347C", SQLite3DataType.INT, 0L)); // 
		triples.add(new StringTestTriple("2019347C", SQLite3DataType.INT, 0L)); // 

		
		for (StringTestTriple triple : triples) {
			Constant castVal = QueryGenerator
					.castToNumeric(Constant.createBinaryConstant(DatatypeConverter.parseHexBinary(triple.value)));
			assertEquals(triple.value.toString(), triple.expectedCastValue, castVal.getValue());
		}
	}

	private long castLongConstant(long constant) {
		return QueryGenerator.castToNumeric(Constant.createIntConstant(constant)).asInt();
	}

}
