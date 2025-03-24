package sqlancer;

import org.junit.jupiter.api.Test;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class TestCommonSchema {
    static class TestTable extends AbstractTable<TestTableColumn, TestIndex, GlobalState<?, ?, ?>> {
        TestTable(String name, List<TestTableColumn> columns, List<TestIndex> indexes, boolean isView) {
            super(name, columns, indexes, isView);
        }

        @Override
        public long getNrRows(GlobalState<?, ?, ?> globalState) {
            return 0;
        }
    }

    static class TestTableColumn extends AbstractTableColumn<TestTable, String> {
        TestTableColumn(String name, TestTable table, String type) {
            super(name, table, type);
        }
    }

    static class TestSchema extends AbstractSchema<GlobalState<?, ?, ?>, TestTable> {
        TestSchema(List<TestTable> tables) {
            super(tables);
        }
    }

    static class TestIndex extends TableIndex {
        TestIndex(String name) {
            super(name);
        }
    }

    private TestTable createTestTable(String name, List<TestIndex> indexes, boolean isView, String... columns) {
        List<TestTableColumn> cols = Arrays.stream(columns).map(col -> new TestTableColumn(col, null, "VARCHAR"))
                .collect(Collectors.toList());
        return new TestTable(name, cols, indexes, isView);
    }

    private TestTableColumn createTestColumn(String name, TestTable table, String type) {
        return new TestTableColumn(name, table, type);
    }

    private TestSchema createTestSchema(TestTable... tables) {
        return new TestSchema(Arrays.asList(tables));
    }

    @Test
    void testColumnManagement() {
        TestTable table = createTestTable("products", Collections.emptyList(), false, "sku", "price");
        List<String> columnNames = table.getColumns().stream().map(TestTableColumn::getName)
                .collect(Collectors.toList());
        List<String> columnTypes = table.getColumns().stream().map(TestTableColumn::getType)
                .collect(Collectors.toList());
        TestTableColumn randomCol = table.getRandomColumn();

        assertTrue(columnNames.containsAll(Set.of("sku", "price")));
        assertTrue(columnTypes.containsAll(Set.of("VARCHAR")));
        assertTrue(table.getColumns().contains(randomCol));
    }

    @Test
    void testIndexManagement() {
        TestIndex idx1 = new TestIndex("idx_sku");
        TestIndex idx2 = new TestIndex("idx_price");
        TestTable table = createTestTable("products", Arrays.asList(idx1, idx2), false, "sku", "price");
        TableIndex randomIndex = table.getRandomIndex();

        assertTrue(table.hasIndexes());
        assertEquals(2, table.getIndexes().size());
        assertTrue(table.getIndexes().contains(randomIndex));
    }

    @Test
    void testViewManagement() {
        TestTable view1 = createTestTable("v1", Collections.emptyList(), true, "col1");
        TestTable view2 = createTestTable("v2", Collections.emptyList(), true, "col2");
        TestTable table = createTestTable("t1", Collections.emptyList(), false, "col3");
        TestSchema schema = createTestSchema(view1, view2, table);

        assertAll(() -> assertEquals(2, schema.getViews().size(), "Should detect 2 views"),
                () -> assertEquals(1, schema.getDatabaseTablesWithoutViews().size(), "Should detect 1 normal table"),
                () -> assertEquals("t1", schema.getDatabaseTablesWithoutViews().get(0).getName()));
    }

    @Test
    void testFreeColumnNameGeneration() {
        TestTable table = createTestTable("users", Collections.emptyList(), false, "id", "name");
        Set<String> generatedNames = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            String newName = table.getFreeColumnName();
            assertTrue(generatedNames.add(newName), "Duplicate: " + newName);

            List<TestTableColumn> newColumns = new ArrayList<>(table.getColumns());
            newColumns.add(new TestTableColumn(newName, table, "TEXT"));
            table = new TestTable(table.getName(), newColumns, table.getIndexes(), table.isView());
        }
    }

    @Test
    void testObjectComparison() {
        TestTable tableA = createTestTable("A", Collections.emptyList(), false, "x", "y");
        TestTable tableB = createTestTable("B", Collections.emptyList(), false, "b");

        TestTableColumn colA1 = new TestTableColumn("x", tableA, "INT");
        TestTableColumn colA2 = new TestTableColumn("y", tableA, "INT");
        TestTableColumn colB1 = new TestTableColumn("b", tableB, "TEXT");

        assertAll(() -> assertTrue(colA1.compareTo(colA2) < 0, "Columns should be ordered by name"),
                () -> assertTrue(colA1.compareTo(colB1) < 0, "Columns should be ordered by name"),
                () -> assertTrue(tableA.compareTo(tableB) > 0, "Tables should be ordered reverse-alphabetically"),
                () -> assertEquals(0, tableA.compareTo(tableA), "Same table should be equal"));
    }

    @Test
    void testEquality() {
        TestTable table1 = createTestTable("t1", Collections.emptyList(), false, "id");
        TestTable table2 = createTestTable("t2", Collections.emptyList(), false, "id");

        TestTableColumn col1 = new TestTableColumn("id", table1, "INT");
        TestTableColumn col2 = new TestTableColumn("id", table1, "INT");
        TestTableColumn col3 = new TestTableColumn("id", table2, "INT");
        TestTableColumn col4 = new TestTableColumn("name", table1, "TEXT");

        assertAll(() -> assertEquals(col1, col2, "Same table/column should be equal"),
                () -> assertNotEquals(col1, col3, "Different tables should not be equal"),
                () -> assertNotEquals(col1, col4, "Different columns should not be equal"),
                () -> assertNotEquals(col1, "invalid_object", "Different types should not be equal"));
    }

    @Test
    void testBoundaryConditions() {
        String longName = "a".repeat(256);
        TestTableColumn longCol = new TestTableColumn(longName, null, "TEXT");
        assertEquals(longName, longCol.getName());

        TestTableColumn col2 = createTestColumn("orphan", null, "UNKNOWN");
        assertEquals("orphan", col2.getFullQualifiedName());
        assertNull(col2.getTable());
    }
}
