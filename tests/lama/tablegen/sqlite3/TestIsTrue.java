package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.gen.SQLite3Cast;

public class TestIsTrue {
	
	@Test
	public void testEmptyString() {
		SQLite3Constant text = SQLite3Constant.createTextConstant("");
		assertTrue(!SQLite3Cast.isTrue(text).get());
	}

}
