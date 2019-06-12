package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;

public class TestGlob {
	
	void test(String val1, String val2, boolean matches) {
		SQLite3Constant t1 = SQLite3Constant.createTextConstant(val1);
		SQLite3Constant t2 = SQLite3Constant.createTextConstant(val2);
		BinaryComparisonOperation glob = new BinaryComparisonOperation(t1, t2, BinaryComparisonOperator.GLOB);
		assertEquals(matches ? 1 : 0, glob.getExpectedValue().asInt());
	}
	
	@Test // SELECT 'asdf' GLOB 'asdf*'; -- 1
	public void test0() {
		test("asdf", "asdf*", true);
	}
	
	@Test // SELECT 'a' GLOB 'a'; -- 1
	public void test1() {
		test("a", "a", true);
	}
	
	@Test // SELECT 'asdf' GLOB '????' -- 1
	public void test2() {
		test("asdf", "????", true);
	}
	
	@Test // SELECT 'asdf' GLOB 'a*f'
	public void test3() {
		test("asdf", "a*f", true);
	}
	
	@Test // SELECT 'asdf' GLOB '*df'
	public void test4() {
		test("asdf", "*df", true);
	}
	
	@Test // SELECT 'asdf' GLOB '*??f'
	public void test5() {
		test("asdf", "*??f", true);
	}
	
	@Test // SELECT 'asdf' GLOB '***???f'
	public void test6() {
		test("asdf", "***???f", true);
	}
	
	@Test // SELECT 'a' GLOB ''; -- 0
	public void test7() {
		test("a", "", false);
	}
	
	@Test // SELECT '' GLOB ''; -- 1
	public void test8() {
		test("", "", true);
	}
	
	@Test // SELECT 'asdf' GLOB '[a-z]sd[a-z]'
	public void test9() {
		test("asdf", "[a-z]sd[a-z]", true);
	}
	
	@Test // SELECT 'asdf' GLOB '[a-z]sd'
	public void test10() {
		test("asdf", "[a-z]sd", false);
	}
	
	@Test // SELECT '[' GLOB '['
	public void test11() {
		test("[", "[", false);
	}
	
	@Test // SELECT '[' GLOB '[[]'
	public void test12() {
		test("[", "[[]", true);
	}
	
	@Test // SELECT 'd' GLOB '[^abc]'
	public void test13() {
		test("d", "[^abc]", true);
	}
	
	@Test // SELECT 'a' GLOB '[^abc]*'
	public void test14() {
		test("a", "[^abc]*", false);
	}
	
	@Test // SELECT '' GLOB '[]'
	public void test115() {
		test("", "[]", false);
	}
	
	@Test // SELECT 'f' GLOB '[abc[def]'
	public void test16() {
		test("f", "[abc[def]", true);
	}
	
	@Test // SELECT '-' GLOB '[-]'
	public void test17() {
		test("-", "[-]", true);
	}
	
	@Test // SELECT '-' GLOB '[a-]'
	public void test18() {
		test("-", "[a-]", true);
	}
	
	@Test // SELECT 'a' GLOB '[a-z]'
	public void test19() {
		test("a", "[a-z]", true);
	}
	
	@Test // SELECT 'a' GLOB '[a-z]'
	public void test20() {
		test("z", "[a-z]", true);
	}
	
	@Test // SELECT 'asdfz' GLOB '[a-z]*[a-z]'
	public void test21() {
		test("asdfz", "[a-z]*[a-z]", true);
	}
	
	@Test // SELECT NULL GLOB 'a';
	public void testNull1() {
		var op = new BinaryComparisonOperation(SQLite3Constant.createNullConstant(), SQLite3Constant.createTextConstant(""), BinaryComparisonOperator.GLOB);
		assertTrue(op.getExpectedValue().isNull());		
	}
	
	@Test // SELECT 'a' GLOB NULL;
	public void testNull2() {
		var op = new BinaryComparisonOperation(SQLite3Constant.createTextConstant("a"), SQLite3Constant.createNullConstant(), BinaryComparisonOperator.GLOB);
		assertTrue(op.getExpectedValue().isNull());		
	}

	
	
}
