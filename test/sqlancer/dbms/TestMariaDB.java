package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.ast.MariaDBColumnName;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mariadb.ast.MariaDBTableReference;
import sqlancer.mariadb.ast.MariaDBVisitor;

public class TestMariaDB {

    @Test
    public void testSelectAsString() {
        MariaDBSchema.MariaDBColumn c0 = new MariaDBSchema.MariaDBColumn("c0", MariaDBSchema.MariaDBDataType.INT, true,
                0);
        MariaDBSchema.MariaDBColumn c1 = new MariaDBSchema.MariaDBColumn("c1", MariaDBSchema.MariaDBDataType.INT, false,
                0);
        List<MariaDBSchema.MariaDBColumn> columns = List.of(c0, c1);
        List<MariaDBSchema.MariaDBIndex> indices = List.of();
        MariaDBSchema.MariaDBTable t1 = new MariaDBSchema.MariaDBTable("t1", columns, indices,
                MariaDBSchema.MariaDBTable.MariaDBEngine.INNO_DB);
        MariaDBSchema.MariaDBTables tables = new MariaDBSchema.MariaDBTables(List.of(t1));

        MariaDBSelectStatement select = new MariaDBSelectStatement();
        select.setFetchColumns(tables.getColumns().stream().map(MariaDBColumnName::new).collect(Collectors.toList()));
        select.setFromList(tables.getTables().stream().map(MariaDBTableReference::new).collect(Collectors.toList()));

        String selectString = MariaDBVisitor.asString(select);
        assertEquals("SELECT c0, c1 FROM t1", selectString);
    }

    @Test
    public void testMariaDB() {
        String mariaDBAvailable = System.getenv("MARIADB_AVAILABLE");
        boolean mariaDBIsAvailable = mariaDBAvailable != null && mariaDBAvailable.equalsIgnoreCase("true");
        assumeTrue(mariaDBIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-queries", "0", "mariadb" }));
    }

}
