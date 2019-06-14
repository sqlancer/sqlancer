package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;

public class TestLike {
	
	@Test
	public void testCaseInsensitive() {
		test("aBc", "Abc", true);
	}

	void test(String val1, String val2, boolean matches) {
		SQLite3Constant t1 = SQLite3Constant.createTextConstant(val1);
		SQLite3Constant t2 = SQLite3Constant.createTextConstant(val2);
		BinaryComparisonOperation glob = new BinaryComparisonOperation(t1, t2, BinaryComparisonOperator.LIKE);
		assertEquals(matches ? 1 : 0, glob.getExpectedValue().asInt());
	}
	
}
