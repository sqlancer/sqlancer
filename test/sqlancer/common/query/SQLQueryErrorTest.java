package sqlancer.common.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SQLQueryErrorTest {
    @Test
    public void testSettersAndGetters() {
        SQLQueryError error = new SQLQueryError();
        error.setLevel(SQLQueryError.ErrorLevel.ERROR);
        error.setCode(123);
        error.setMessage("Test message");
        assertEquals(SQLQueryError.ErrorLevel.ERROR, error.getLevel());
        assertEquals(123, error.getCode());
        assertEquals("Test message", error.getMessage());
    }

    @Test
    public void testHasSameLevel() {
        SQLQueryError e1 = new SQLQueryError();
        SQLQueryError e2 = new SQLQueryError();
        e1.setLevel(SQLQueryError.ErrorLevel.WARNING);
        e2.setLevel(SQLQueryError.ErrorLevel.WARNING);
        assertTrue(e1.hasSameLevel(e2));
        e2.setLevel(SQLQueryError.ErrorLevel.ERROR);
        assertFalse(e1.hasSameLevel(e2));
    }

    @Test
    public void testHasSameCodeAndMessage() {
        SQLQueryError e1 = new SQLQueryError();
        SQLQueryError e2 = new SQLQueryError();
        e1.setCode(1);
        e2.setCode(1);
        e1.setMessage("msg");
        e2.setMessage("msg");
        assertTrue(e1.hasSameCodeAndMessage(e2));
        e2.setCode(2);
        assertFalse(e1.hasSameCodeAndMessage(e2));
        e2.setCode(1);
        e2.setMessage("other");
        assertFalse(e1.hasSameCodeAndMessage(e2));
    }

    @Test
    public void testEquals() {
        SQLQueryError e1 = new SQLQueryError();
        SQLQueryError e2 = new SQLQueryError();
        e1.setLevel(SQLQueryError.ErrorLevel.ERROR);
        e1.setCode(1);
        e1.setMessage("msg");
        e2.setLevel(SQLQueryError.ErrorLevel.ERROR);
        e2.setCode(1);
        e2.setMessage("msg");
        assertEquals(e1, e2);
        e2.setLevel(SQLQueryError.ErrorLevel.WARNING);
        assertNotEquals(e1, e2);
    }

    @Test
    public void testToString() {
        SQLQueryError e = new SQLQueryError();
        e.setLevel(SQLQueryError.ErrorLevel.ERROR);
        e.setCode(1);
        e.setMessage("msg");
        String str = e.toString();
        assertTrue(str.contains("Level: ERROR"));
        assertTrue(str.contains("Code: 1"));
        assertTrue(str.contains("Message: msg"));
    }

    @Test
    public void testCompareTo() {
        SQLQueryError e1 = new SQLQueryError();
        SQLQueryError e2 = new SQLQueryError();
        e1.setCode(1);
        e2.setCode(2);
        assertTrue(e1.compareTo(e2) < 0);
        e2.setCode(1);
        e1.setLevel(SQLQueryError.ErrorLevel.ERROR);
        e2.setLevel(SQLQueryError.ErrorLevel.WARNING);
        assertTrue(e1.compareTo(e2) > 0 || e1.compareTo(e2) < 0);
        e2.setLevel(SQLQueryError.ErrorLevel.ERROR);
        e1.setMessage("a");
        e2.setMessage("b");
        assertTrue(e1.compareTo(e2) < 0);
    }
}

