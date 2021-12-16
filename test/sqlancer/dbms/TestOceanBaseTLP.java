package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestOceanBaseTLP {

    String oceanBaseAvailable = System.getenv("OCEANBASE_AVAILABLE");
    boolean oceanBaseIsAvailable = oceanBaseAvailable != null && oceanBaseAvailable.equalsIgnoreCase("true");

    @Test
    public void testTLP() {
        assumeTrue(oceanBaseIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--database-prefix", "tlpdb", "--num-queries", TestConfig.NUM_QUERIES,
                        "--username", "sqlancer@test", "--password", "sqlancer",
                        // after deploy oceanbase,if you don't create tenant to test,firstly create tenant test,then
                        // create user sqlancer:
                        // mysql -h127.1 -uroot@test -P2881 -Doceanbase -A -e"create user sqlancer identified by
                        // 'sqlancer';grant all on *.* to sqlancer;"
                        "oceanbase", "--oracle", "TLP_WHERE" }));
    }

}
