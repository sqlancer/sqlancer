package lama.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLUnaryPrefixOperation;
import lama.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

public class MySQLTestNot {

	@Test
	public void testNot() {
		MySQLConstant val = MySQLConstant.createIntConstant(-123);
		MySQLUnaryPrefixOperation not = new MySQLUnaryPrefixOperation(val, MySQLUnaryPrefixOperator.NOT);
		assertEquals(0, not.getExpectedValue().getInt());
	}
	
	
}
