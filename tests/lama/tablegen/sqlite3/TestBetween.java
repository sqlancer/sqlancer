package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3IntConstant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BetweenOperation;

public class TestBetween {
	
	@Test
	public void testBetween() {
		testBetween(SQLite3IntConstant.createIntConstant(5), SQLite3IntConstant.createIntConstant(0), SQLite3IntConstant.createIntConstant(5));
	}
	
	@Test
	public void testBetweenNull1() {
		var op = new BetweenOperation(SQLite3IntConstant.createIntConstant(0), false, SQLite3Constant.createNullConstant(), SQLite3IntConstant.createIntConstant(5));
		assertTrue(op.getExpectedValue().isNull());
	}
	
	@Test
	public void testBetweenNull2() {
		var op = new BetweenOperation(SQLite3Constant.createNullConstant(), false, SQLite3IntConstant.createIntConstant(0), SQLite3IntConstant.createIntConstant(5));
		assertTrue(op.getExpectedValue().isNull());
	}
	
	@Test
	public void testBetweenNull3() {
		var op = new BetweenOperation(SQLite3IntConstant.createIntConstant(5), false, SQLite3IntConstant.createIntConstant(0), SQLite3Constant.createNullConstant());
		assertTrue(op.getExpectedValue().isNull());
	}
	
	public void testBetween(SQLite3Expression x, SQLite3Expression y, SQLite3Expression z) {
		var op = new BetweenOperation(x, false, y, z);
		assertEquals(1, op.getExpectedValue().asInt());
		var opNegated = new BetweenOperation(x, true, y, z);
		assertEquals(0, opNegated.getExpectedValue().asInt());
	}

}
