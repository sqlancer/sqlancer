package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.UnaryOperation;
import lama.sqlite3.ast.UnaryOperation.UnaryOperator;

public class TestUnaryMinus {

	@Test
	public void testPointZero() {
		var textVal = SQLite3Constant.createTextConstant("-566656292.0");
		var minus = new UnaryOperation(UnaryOperator.MINUS, textVal);
		assertEquals(566656292.0, minus.getExpectedValue().asDouble());
	}
	
}
