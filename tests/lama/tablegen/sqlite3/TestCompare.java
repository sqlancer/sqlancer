package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

class TestCompare {

	@Test
	void test1() {
		SQLite3Constant a = SQLite3TextConstant.createTextConstant("a");
		assertEquals(0, a.applyLess(a, CollateSequence.RTRIM).asInt());
	}
	
	@Test // ('a') < ('^') - 0
	void test2() {
		SQLite3Constant a = SQLite3TextConstant.createTextConstant("a");
		assertEquals(0, a.applyLess(SQLite3Constant.createTextConstant("^"), CollateSequence.NOCASE).asInt());
	}
	
	// SELECT ((1) BETWEEN (NULL) AND (0)); -- 0 
	// SELECT ((-1) BETWEEN (NULL) AND (0)); -- NULL

	
	

}
