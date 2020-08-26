package sqlancer.pqs.sqlite.cast;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import sqlancer.sqlite3.ast.SQLite3Constant;

public class TestCastToBoolean {

    @Test
    void nan() {
        SQLite3Constant text = SQLite3Constant.createTextConstant("NaN");
        assertEquals(text.castToBoolean().asInt(), 0);
    }

}
