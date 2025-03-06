package sqlancer.mysql;

import org.junit.jupiter.api.Test;
import sqlancer.Main;

import static org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.cms.RecipientId.password;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MySQLTesting {
    @Test
    public void testMySQLNoRECOracle() {
        assertEquals(0, Main.executeMain("--num-threads", "9",
                "--num-tries", "100", "--num-queries", "5000", "--max-generated-databases", "1",
                "--host", "localhost", "--port", String.valueOf("3306"), "--username",
                "springstudent", "--password", "springstudent",
                "mysql", "--oracle", "DQP"));
    }
}
