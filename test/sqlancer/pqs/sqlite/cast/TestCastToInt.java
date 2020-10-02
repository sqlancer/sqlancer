package sqlancer.pqs.sqlite.cast;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import sqlancer.sqlite3.ast.SQLite3Cast;
import sqlancer.sqlite3.ast.SQLite3Constant;

class TestCastToInt {

    @Test
    void test1() {
        assertBinaryCastToInt("dbb25259", 0);
    }

    @Test
    void test2() {
        assertBinaryCastToInt("d9a3", 0);
    }

    @Test
    void test3() {
        assertCastStringToInt("1231231922047954197746780200000", Long.MAX_VALUE);
    }

    @Test
    void test4() {
        assertCastStringToInt("1231231922047954197746780200000.5", Long.MAX_VALUE);
    }

    @Test
    void test5() {
        assertCastStringToInt("-1231231922047954197746780200000.5", Long.MIN_VALUE);
    }

    @Test
    void testSign1() {
        assertCastStringToInt("++123", 0);
    }

    @Test
    void testSign2() {
        assertCastStringToInt("+123", 123);
    }

    @Test
    void testSign3() {
        assertCastStringToInt("-123", -123);
    }

    @Test
    void testSign4() {
        assertCastStringToInt("-+123", 0);
    }

    @Test
    void testSign5() {
        assertCastStringToInt("+-123", 0);
    }

    @Test
    void testInfinity1() {
        assertCastStringToInt("Infinity", 0);
    }

    @Test
    void testInfinity2() {
        assertCastStringToInt("-Infinity", 0);
    }

    @Test
    void testNan() {
        assertCastStringToInt("NaN", 0);
    }

    void assertCastStringToInt(String val, long expectedLong) {
        SQLite3Constant c = SQLite3Constant.createTextConstant(val);
        SQLite3Constant intVal = SQLite3Cast.castToInt(c);
        assertEquals(intVal.asInt(), expectedLong);
    }

    void assertBinaryCastToInt(String val, long expectedLong) {
        SQLite3Constant c = SQLite3Constant.createBinaryConstant(val);
        SQLite3Constant intVal = SQLite3Cast.castToInt(c);
        assertEquals(intVal.asInt(), expectedLong);
    }

}
