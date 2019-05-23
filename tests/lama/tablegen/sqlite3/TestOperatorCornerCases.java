package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation.UnaryOperator;

public class TestOperatorCornerCases {
	
	@Test
	public void testUnaryMinValueMinus() {
		SQLite3Constant val = SQLite3Constant.createIntConstant(-9223372036854775808L);
		UnaryOperation op = new UnaryOperation(UnaryOperator.MINUS, val);
		assertEquals(op.getExpectedValue().asDouble(), -(double) Long.MIN_VALUE);
	}

}
