package lama.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.mysql.ast.MySQLCastOperation;
import lama.mysql.ast.MySQLCastOperation.CastType;
import lama.mysql.ast.MySQLConstant;

public class MySQLEqualsTest {
	
	@Test
	public void test() {
		var minusOne = MySQLConstant.createIntConstant(-1);
		var unsignedCast = new MySQLCastOperation(minusOne, CastType.UNSIGNED);
		var result = minusOne.isEquals(unsignedCast.getExpectedValue());
		assertEquals(0, result.getInt());
	}

}
