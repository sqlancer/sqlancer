package lama.tablegen.sqlite3;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;

public class TestAdd {

	SQLite3Constant intThree = SQLite3Constant.createIntConstant(3);
	SQLite3Constant stringA = SQLite3Constant.createTextConstant("a");
	SQLite3Constant intMax = SQLite3Constant.createIntConstant(9223372036854775807L);
	SQLite3Constant nullVal = SQLite3Constant.createNullConstant();

	@Test // SELECT 3 + 3; -- 6
	public void test1() {
		BinaryOperation binOp = BinaryOperation.create(intThree, intThree, BinaryOperator.PLUS);
		assertEquals(binOp.getExpectedValue().asInt(), 6);
	}

	@Test // SELECT 3 + a; -- 3
	public void test2() {
		BinaryOperation binOp = BinaryOperation.create(intThree, intThree, BinaryOperator.PLUS);
		assertEquals(binOp.getExpectedValue().asInt(), 6);
	}
	
	@Test // SELECT 3 + 9223372036854775807; -- 3
	public void test3() {
		BinaryOperation binOp = BinaryOperation.create(intThree, intMax, BinaryOperator.PLUS);
		assertEquals(binOp.getExpectedValue().asDouble(), 3d + 9223372036854775807L);
	}
	
	@Test // SELECT 3 + NULL; -- NULL
	public void test4() {
		BinaryOperation binOp = BinaryOperation.create(intThree, nullVal, BinaryOperator.PLUS);
		assertTrue(binOp.getExpectedValue().isNull());
	}
	
	@Test // SELECT NULL + 3; -- NULL
	public void test5() {
		BinaryOperation binOp = BinaryOperation.create(nullVal, intThree, BinaryOperator.PLUS);
		assertTrue(binOp.getExpectedValue().isNull());
	}

}
