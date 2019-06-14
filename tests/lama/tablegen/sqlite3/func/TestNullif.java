package lama.tablegen.sqlite3.func;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Function;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

public class TestNullif {

	@Test
	public void differentInts() {
		SQLite3Constant intVal = SQLite3Constant.createIntConstant(5);
		assertTrue(ComputableFunction.NULLIF.apply(intVal, intVal).isNull());
	}

	@Test
	public void testImplicitAffinity() {
		ColumnName c1 = new ColumnName(new Column("c0", SQLite3DataType.NONE, false, false, CollateSequence.RTRIM),
				SQLite3TextConstant.createTextConstant(" "));
		ColumnName c2 = new ColumnName(new Column("c0", SQLite3DataType.NONE, false, false, null),
				SQLite3TextConstant.createTextConstant(""));
		var func = new SQLite3Function(ComputableFunction.NULLIF, c1, c2);
		assertTrue(func.getExpectedValue().isNull());

	}

}
