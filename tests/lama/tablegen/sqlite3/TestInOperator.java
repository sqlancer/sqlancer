package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.CollateOperation;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.InOperation;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral.Type;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

public class TestInOperator {

	private SQLite3Expression numericAffinityTwo = new Cast(new TypeLiteral(Type.NUMERIC),
			SQLite3Constant.createIntConstant(2));
	SQLite3Constant one = SQLite3Constant.createIntConstant(1);
	SQLite3Constant two = SQLite3Constant.createIntConstant(2);
	SQLite3Constant three = SQLite3Constant.createIntConstant(3);
	SQLite3Constant textOne = SQLite3Constant.createTextConstant("1");
	SQLite3Constant textTwo = SQLite3Constant.createTextConstant("2");
	SQLite3Constant textThree = SQLite3Constant.createTextConstant("3");
	SQLite3Constant textA = SQLite3Constant.createTextConstant("A");
	SQLite3Constant texta = SQLite3Constant.createTextConstant("a");
	SQLite3Expression columnNoCaseAffinity = new ColumnName(
			new Column("c0", SQLite3DataType.TEXT, false, false, CollateSequence.NOCASE), texta);
	SQLite3Expression columnIntTextAffinity = new ColumnName(
			new Column("c0", SQLite3DataType.TEXT, false, false, CollateSequence.NOCASE),
			SQLite3Constant.createTextConstant("-1004118087"));

	@Test
	public void test() {
		var inOp = new InOperation(columnIntTextAffinity, Arrays
				.asList(new Cast(new TypeLiteral(Type.INTEGER), SQLite3Constant.createTextConstant("-1004118087.0"))));
		assertEquals(1, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT CAST(2 AS NUMERIC) in ('1', '2', '3') -- 1
	public void testApplyAffinity1() {
		var inOp = new InOperation(numericAffinityTwo, Arrays.asList(textOne, textTwo, textThree));
		assertEquals(1, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT 3 in ('2', '3') -- 0
	public void testApplyAffinity2() {
		var inOp = new InOperation(three, Arrays.asList(textTwo, textThree));
		assertEquals(0, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT '2' in (CAST(2 AS NUMERIC)) -- 0
	public void testApplyAffinity3() {
		var inOp = new InOperation(textTwo, Arrays.asList(numericAffinityTwo));
		assertEquals(0, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT 3 in (1, 2, 3) -- 1
	public void testContains() {
		var inOp = new InOperation(three, Arrays.asList(one, two, three));
		assertEquals(1, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT CAST(2 AS NUMERIC) in ('1', '3') -- 1
	public void testNotContains() {
		var inOp = new InOperation(numericAffinityTwo, Arrays.asList(textOne, textThree));
		assertEquals(0, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT 'a' in ('A') -- 0
	public void testCollate1() {
		var inOp = new InOperation(texta, Arrays.asList(textA));
		assertEquals(0, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT 'a' COLLATE NOCASE in ('A') -- 1
	public void testCollate2() {
		var inOp = new InOperation(new CollateOperation(texta, CollateSequence.NOCASE), Arrays.asList(textA));
		assertEquals(1, inOp.getExpectedValue().asInt());
	}

	@Test // SELECT 'a' in ('A' COLLATE NOCASE) -- 0
	public void testCollate3() {
		var inOp = new InOperation(texta, Arrays.asList(new CollateOperation(textA, CollateSequence.NOCASE)));
		assertEquals(0, inOp.getExpectedValue().asInt());
	}

	@Test
	// CREATE TABLE t0(c0 TEXT COLLATE NOCASE);
	// INSERT INTO t0 VALUES ('a');
	// SELECT c0 in ('A') FROM t0; -- 1
	public void testCollate4() {
		var inOp = new InOperation(columnNoCaseAffinity, Arrays.asList(textA));
		assertEquals(1, inOp.getExpectedValue().asInt());
	}

	@Test
	// CREATE TABLE t0(c0 TEXT COLLATE NOCASE);
	// INSERT INTO t0 VALUES ('a');
	// SELECT 'A' in (c0) FROM t0; -- 0
	public void testCollate5() {
		var inOp = new InOperation(textA, Arrays.asList(columnNoCaseAffinity));
		assertEquals(0, inOp.getExpectedValue().asInt());
	}
	
	@Test
	// SELECT CAST('4.0' AS TEXT) IN (CAST('4.0' AS REAL)); -- 1
	public void testAffinity() {
		var inOp = new InOperation(new Cast(new TypeLiteral(Type.TEXT), SQLite3Constant.createTextConstant("-1600763882.0")), Arrays.asList(new Cast(new TypeLiteral(Type.REAL), SQLite3Constant.createTextConstant("-1600763882.0"))));
		assertEquals(1, inOp.getExpectedValue().asInt());
	}

}
