package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestOceanBasePQS {

    @Test
    public void testPQS() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.OCEANBASE_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--random-string-generation", "ALPHANUMERIC_SPECIALCHAR",
                        "--database-prefix", "pqsdb", "--num-queries", TestConfig.NUM_QUERIES, "--username",
                        "sqlancer@test", "--password", "sqlancer",
                        // after deploy oceanbase,if you don't create tenant to test,firstly create tenant test,then
                        // create user sqlancer:
                        // mysql -h127.1 -uroot@test -P2881 -Doceanbase -A -e"create user sqlancer identified by
                        // 'sqlancer';grant all on *.* to sqlancer;"
                        "oceanbase", "--oracle", "PQS" }));
    }

}
