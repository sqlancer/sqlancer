package lama.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLUnaryNotOperator;

public class MySQLTestNot {

	@Test
	public void testNot() {
		MySQLConstant val = MySQLConstant.createIntConstant(-123);
		MySQLUnaryNotOperator not = new MySQLUnaryNotOperator(val);
		assertEquals(0, not.getExpectedValue().getInt());
	}
	
	
}
