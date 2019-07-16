package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3IntConstant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.UnaryOperation;
import lama.sqlite3.ast.UnaryOperation.UnaryOperator;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

public class TestApplyEquals {

	SQLite3Constant positiveInf = SQLite3Constant.createRealConstant(Double.POSITIVE_INFINITY);
	SQLite3Constant negativeInf = SQLite3Constant.createRealConstant(Double.NEGATIVE_INFINITY);
	private SQLite3Constant three = SQLite3Constant.createIntConstant(3);
	private SQLite3Constant zero = SQLite3Constant.createIntConstant(0);
	private SQLite3Constant textZero = SQLite3Constant.createTextConstant("0");
	private SQLite3Constant textThree = SQLite3Constant.createTextConstant("3");
	private ColumnName column = new ColumnName(new Column("c0", SQLite3DataType.TEXT, false, true, null), SQLite3Constant.createBinaryConstant("a5d848c9"));

	@Test
	public void testNumbertextEquals1() {
		assertEquals(0, three.applyEquals(textThree).asInt());
	}

	@Test
	public void testNumbertextEquals2() {
		assertEquals(0, textZero.applyEquals(zero).asInt());
	}

	@Test // SELECT 1e500 = 3;
	public void testInfInt1() {
		assertEquals(0, positiveInf.applyEquals(three).asInt());
	}

	@Test // SELECT 3 = 1e500;
	public void testInfInt2() {
		assertEquals(0, three.applyEquals(positiveInf).asInt());
	}

	@Test // SELECT 1e500 = 1e500;
	public void testInfInf() {
		assertEquals(1, positiveInf.applyEquals(positiveInf).asInt());
	}

	@Test // SELECT 1e500 = -1e500; -- 0
	public void testInfNegativeInf() {
		assertEquals(0, positiveInf.applyEquals(negativeInf).asInt());
	}
	
	@Test // + t1.c0 = t1.c0
	public void testApplyColumnEqualsInf() {
		BinaryComparisonOperation isExpr = new BinaryComparisonOperation(new UnaryOperation(UnaryOperator.PLUS, column), column, BinaryComparisonOperator.IS);
		assertEquals(1, isExpr.getExpectedValue().asInt());
	}
	
	@Test
	public void testNotEquals() {
		SQLite3Constant createTextConstant = SQLite3TextConstant.createTextConstant("-21779103");
		SQLite3Constant createIntConstant = SQLite3IntConstant.createIntConstant(0xfffffffffeb3ad61L);
		SQLite3Expression.BinaryComparisonOperation notEquals = new SQLite3Expression.BinaryComparisonOperation(
				createTextConstant, createIntConstant, BinaryComparisonOperator.NOT_EQUALS);
		assertEquals(1, notEquals.getExpectedValue().asInt());
	}

	@Test
	public void testSameCharacter() {
		assertEqualStrings("a", "a", CollateSequence.BINARY);
		assertEqualStrings("a", "a", CollateSequence.NOCASE);
		assertEqualStrings("a", "a", CollateSequence.RTRIM);
	}

	@Test
	public void testCaseDifferentCharacter() {
		assertNoEqualStrings("a", "A", CollateSequence.BINARY);
		assertEqualStrings("a", "A", CollateSequence.NOCASE);
		assertNoEqualStrings("a", "A", CollateSequence.RTRIM);
	}

	@Test
	public void testSameCharacterRightWhitespace() {
		assertNoEqualStrings("a ", "A ", CollateSequence.BINARY);

		assertNoEqualStrings("a ", "A", CollateSequence.NOCASE);
		assertNoEqualStrings("a", "A ", CollateSequence.NOCASE);

		assertNoEqualStrings("a ", "A", CollateSequence.RTRIM);
		assertNoEqualStrings("a", "A ", CollateSequence.RTRIM);
		assertEqualStrings("a ", "a", CollateSequence.RTRIM);
		assertEqualStrings("a", "a ", CollateSequence.RTRIM);
		assertEqualStrings("a ", "a  ", CollateSequence.RTRIM);

		assertNoEqualStrings(" a", "a", CollateSequence.RTRIM);
	}
	
	@Test
	public void testSpaceRtrim() {
		assertEqualStrings("", " ", CollateSequence.RTRIM);
		assertEqualStrings(" ", "", CollateSequence.RTRIM);
	}
	
	@Test // SELECT ('MhQl' IS ((+ t0.c3))) from t0; -- 1
	public void testEqualsCollate() {
		SQLite3Constant left = SQLite3Constant.createTextConstant("MhQl");
		ColumnName column = new ColumnName(new Column("c0", SQLite3DataType.TEXT, false, true, CollateSequence.NOCASE), SQLite3Constant.createTextConstant("MHQL"));
		BinaryComparisonOperation isExpr = new BinaryComparisonOperation(left, new UnaryOperation(UnaryOperator.PLUS, column), BinaryComparisonOperator.IS);
		assertEquals(1, isExpr.getExpectedValue().asInt());
	}
	
	public void assertEqualStrings(String c1, String c2, CollateSequence collate) {
		SQLite3Constant leftStr = SQLite3Constant.createTextConstant(c1);
		SQLite3Constant rightStr = SQLite3Constant.createTextConstant(c2);
		SQLite3Constant equals = leftStr.applyEquals(rightStr, collate);
		assertEquals(1, equals.asInt());
	}

	public void assertNoEqualStrings(String c1, String c2, CollateSequence collate) {
		SQLite3Constant leftStr = SQLite3Constant.createTextConstant(c1);
		SQLite3Constant rightStr = SQLite3Constant.createTextConstant(c2);
		SQLite3Constant equals = leftStr.applyEquals(rightStr, collate);
		assertEquals(0, equals.asInt());
	}
}
