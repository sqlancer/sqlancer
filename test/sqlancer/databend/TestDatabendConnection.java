package sqlancer.databend;

import com.mongodb.annotations.ThreadSafe;
import org.junit.jupiter.api.Test;
import sqlancer.Main;
import sqlancer.dbms.TestConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDatabendConnection {
    @Test
    void testConnection(){
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix","databend",
                        "--host","192.168.81.133","--port","3307","--username","user1"
                        ,"databend","--oracle","NoREC"}));
    }
}
