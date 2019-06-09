package lama.tablegen.sqlite3.func;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;

public class TestNullif {

	
	@Test
	public void differentInts() {
		SQLite3Constant intVal = SQLite3Constant.createIntConstant(5);
		assertTrue(ComputableFunction.NULLIF.apply(intVal, intVal).isNull());
	}
	
}
