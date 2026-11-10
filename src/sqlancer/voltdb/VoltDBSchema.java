package sqlancer.voltdb;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;

/**
 * The tables of one SQLancer database. A VoltDB cluster has a single schema, so each SQLancer database is the set of
 * tables whose names start with its prefix (see {@link VoltDBProvider#prefix}).
 */
public class VoltDBSchema extends AbstractSchema<VoltDBGlobalState, VoltDBSchema.VoltDBTable> {

    private static final Pattern PARTITION_COLUMN = Pattern.compile("\"partitionColumn\":\"([^\"]+)\"");

    public enum VoltDBDataType {

        TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DECIMAL, VARCHAR, VARBINARY, TIMESTAMP, DATE, BOOLEAN;

        public static VoltDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static VoltDBDataType getRandomPartitionable() {
            return Randomly.fromOptions(TINYINT, SMALLINT, INTEGER, BIGINT, VARCHAR, VARBINARY);
        }

        // VoltDB partitions only on integer, VARCHAR and VARBINARY columns.
        public boolean isPartitionable() {
            switch (this) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case VARCHAR:
            case VARBINARY:
                return true;
            default:
                return false;
            }
        }

        public boolean hasSize() {
            return this == VARCHAR || this == VARBINARY;
        }

        public static VoltDBDataType from(String typeName) {
            return valueOf(typeName.toUpperCase(Locale.ROOT));
        }
    }

    public static class VoltDBCompositeDataType {

        private final VoltDBDataType type;
        private final int size;

        public VoltDBCompositeDataType(VoltDBDataType type, int size) {
            this.type = type;
            this.size = size;
        }

        public static VoltDBCompositeDataType getRandom(VoltDBDataType type) {
            int size = 0;
            if (type.hasSize()) {
                // sizes on both sides of 64 bytes, where VoltDB moves values out of the tuple
                size = Randomly.fromOptions(1, 4, 16, 63, 64, 100);
            }
            return new VoltDBCompositeDataType(type, size);
        }

        public VoltDBDataType getType() {
            return type;
        }

        public int getSize() {
            return size;
        }

        @Override
        public String toString() {
            return type.hasSize() ? type + "(" + size + ")" : type.toString();
        }
    }

    public static class VoltDBColumn extends AbstractTableColumn<VoltDBTable, VoltDBCompositeDataType> {

        private final boolean nullable;

        public VoltDBColumn(String name, VoltDBCompositeDataType type, boolean nullable) {
            super(name, null, type);
            this.nullable = nullable;
        }

        public boolean isNullable() {
            return nullable;
        }
    }

    public static class VoltDBTable extends AbstractRelationalTable<VoltDBColumn, TableIndex, VoltDBGlobalState> {

        private final String partitionColumn;
        private final List<String> primaryKey;

        public VoltDBTable(String name, List<VoltDBColumn> columns, String partitionColumn, List<String> primaryKey) {
            super(name, columns, Collections.emptyList(), false);
            this.partitionColumn = partitionColumn;
            this.primaryKey = primaryKey;
        }

        // The partitioning column's name, or null for a replicated table.
        public String getPartitionColumn() {
            return partitionColumn;
        }

        public boolean isPartitioned() {
            return partitionColumn != null;
        }

        // The primary key's column names, empty when the table has none. VoltDB can UPSERT only into a table with one.
        public List<String> getPrimaryKey() {
            return primaryKey;
        }
    }

    public VoltDBSchema(List<VoltDBTable> tables) {
        super(tables);
    }

    public static VoltDBSchema fromConnection(SQLConnection con, String prefix) throws SQLException {
        DatabaseMetaData md = ((VoltDBConnection) con).getMetaData();
        List<VoltDBTable> tables = new ArrayList<>();
        for (String[] t : getTables(md, prefix)) {
            List<VoltDBColumn> columns = getColumns(md, t[0]);
            VoltDBTable table = new VoltDBTable(t[0], columns, t[1], getPrimaryKey(md, t[0]));
            for (VoltDBColumn c : columns) {
                c.setTable(table);
            }
            tables.add(table);
        }
        return new VoltDBSchema(tables);
    }

    // Returns {name, partition column or null} for each of this database's tables.
    static List<String[]> getTables(DatabaseMetaData md, String prefix) throws SQLException {
        List<String[]> tables = new ArrayList<>();
        try (ResultSet rs = md.getTables(null, null, null, new String[] { "TABLE" })) {
            while (rs.next()) {
                String name = rs.getString("TABLE_NAME");
                // compare in Java: '_' in the prefix is a LIKE wildcard. The GEO oracle's table, whose GEOGRAPHY
                // columns no other oracle reads, is left out
                if (!name.startsWith(prefix) || name.equals(prefix + "GEO")) {
                    continue;
                }
                String remarks = rs.getString("REMARKS");
                Matcher m = remarks == null ? null : PARTITION_COLUMN.matcher(remarks);
                tables.add(new String[] { name, m != null && m.find() ? m.group(1) : null });
            }
        }
        return tables;
    }

    private static List<String> getPrimaryKey(DatabaseMetaData md, String table) throws SQLException {
        List<String> key = new ArrayList<>();
        try (ResultSet rs = md.getPrimaryKeys(null, null, table)) {
            while (rs.next()) {
                if (table.equals(rs.getString("TABLE_NAME"))) {
                    key.add(rs.getString("COLUMN_NAME"));
                }
            }
        }
        return key;
    }

    private static List<VoltDBColumn> getColumns(DatabaseMetaData md, String table) throws SQLException {
        List<VoltDBColumn> columns = new ArrayList<>();
        try (ResultSet rs = md.getColumns(null, null, table, null)) {
            while (rs.next()) {
                if (!table.equals(rs.getString("TABLE_NAME"))) {
                    continue;
                }
                VoltDBDataType type = VoltDBDataType.from(rs.getString("TYPE_NAME"));
                int size = type.hasSize() ? rs.getInt("COLUMN_SIZE") : 0;
                columns.add(new VoltDBColumn(rs.getString("COLUMN_NAME"), new VoltDBCompositeDataType(type, size),
                        "YES".equals(rs.getString("IS_NULLABLE"))));
            }
        }
        return columns;
    }

}
