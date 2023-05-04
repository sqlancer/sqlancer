package sqlancer.qpg.cockroachdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.dbms.TestConfig;

public class TestMaterializeQPG {

    @Test
    public void testMaterializeQPG() {
        String materialize = System.getenv("MATERIALIZE_AVAILABLE");
        boolean materializeIsAvailable = materialize != null && materialize.equalsIgnoreCase("true");
        assumeTrue(materializeIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-threads", "4", "--qpg-enable", "true", "--num-queries", TestConfig.NUM_QUERIES, "--username",
                "materialize", "materialize", "--oracle", "QUERY_PARTITIONING", "--set-max-tables-mvs", "true" }));
    }

}
