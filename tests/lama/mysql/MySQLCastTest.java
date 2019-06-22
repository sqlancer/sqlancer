package lama.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.mysql.ast.MySQLCastOperation;
import lama.mysql.ast.MySQLConstant;

public class MySQLCastTest {
	
	@Test
	public void testWhiteSpace() {
		var cast = new MySQLCastOperation(MySQLConstant.createStringConstant("  60y"), MySQLCastOperation.CastType.SIGNED);
		assertEquals(60, cast.getExpectedValue().getInt());
	}
	
	@Test
	public void testTab() {
		var cast = new MySQLCastOperation(MySQLConstant.createStringConstant("\t60y"), MySQLCastOperation.CastType.SIGNED);
		assertEquals(60, cast.getExpectedValue().getInt());
	}
	
}
