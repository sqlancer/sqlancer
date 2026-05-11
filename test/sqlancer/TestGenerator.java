package sqlancer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.duckdb.DuckDBSchema;
import sqlancer.simple.dialect.SimpleDialect;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.SelectGenerator;

public class TestGenerator {

    @Test
    public void testEmpty() {
        SimpleDialect dialect = new SimpleDialect();

        List<DuckDBSchema.DuckDBColumn> columns = List.of(new DuckDBSchema.DuckDBColumn("c0", null, false, false),
                new DuckDBSchema.DuckDBColumn("c1", null, false, false));
        List<DuckDBSchema.DuckDBTable> tables = List.of(new DuckDBSchema.DuckDBTable("t0", columns, false),
                new DuckDBSchema.DuckDBTable("t1", columns, false));

        Generator gen = new SelectGenerator<>(dialect, tables, new Randomly(0), 1);

        for (int i = 0; i < 1000; i++) {
            assertDoesNotThrow(() -> {
                System.out.println(dialect.generateSelect(gen).print());
                gen.reset();
            });
        }
    }
}
