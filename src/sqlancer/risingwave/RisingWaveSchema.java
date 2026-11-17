package sqlancer.risingwave;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;

public class RisingWaveSchema extends AbstractSchema<RisingWaveGlobalState, RisingWaveSchema.RisingWaveTable> {
    public enum RelationKind {
        TABLE, VIEW, MATERIALIZED_VIEW
    }

    public RisingWaveSchema(List<RisingWaveTable> tables) {
        super(tables);
    }

    public static final class RisingWaveColumn extends AbstractTableColumn<RisingWaveTable, RisingWaveType> {
        private final boolean nullable;

        public RisingWaveColumn(String name, RisingWaveType type) {
            this(name, type, true);
        }

        public RisingWaveColumn(String name, RisingWaveType type, boolean nullable) {
            super(name, null, type);
            this.nullable = nullable;
        }

        public boolean isNullable() {
            return nullable;
        }
    }

    public static final class RisingWaveTable
            extends AbstractRelationalTable<RisingWaveColumn, TableIndex, RisingWaveGlobalState> {
        private final RisingWaveColumn initialJoinKeyColumn;
        private final RelationKind kind;

        public RisingWaveTable(String name, List<RisingWaveColumn> columns) {
            this(name, columns, null);
        }

        public RisingWaveTable(String name, List<RisingWaveColumn> columns, RisingWaveColumn initialJoinKeyColumn) {
            this(name, columns, initialJoinKeyColumn, RelationKind.TABLE);
        }

        public RisingWaveTable(String name, List<RisingWaveColumn> columns, RisingWaveColumn initialJoinKeyColumn,
                RelationKind kind) {
            super(name, columns, Collections.emptyList(), kind != RelationKind.TABLE);
            this.kind = kind;
            if (initialJoinKeyColumn != null && !columns.contains(initialJoinKeyColumn)) {
                throw new IllegalArgumentException("Initial join key must belong to its table");
            }
            this.initialJoinKeyColumn = initialJoinKeyColumn;
            columns.forEach(c -> c.setTable(this));
        }

        public RisingWaveColumn getInitialJoinKeyColumn() {
            return initialJoinKeyColumn;
        }

        public RelationKind getKind() {
            return kind;
        }

        public boolean isWritable() {
            return kind == RelationKind.TABLE;
        }
    }

    public static RisingWaveSchema read(SQLConnection connection) throws SQLException {
        Map<String, List<RisingWaveColumn>> tables = new LinkedHashMap<>();
        Map<String, RelationKind> kinds = new LinkedHashMap<>();
        String sql = "SELECT r.name, c.name, c.data_type, c.is_nullable, r.kind FROM ("
                + "SELECT id, name, schema_id, 'TABLE' AS kind FROM rw_catalog.rw_tables UNION ALL "
                + "SELECT id, name, schema_id, 'VIEW' AS kind FROM rw_catalog.rw_views UNION ALL "
                + "SELECT id, name, schema_id, 'MATERIALIZED_VIEW' AS kind FROM rw_catalog.rw_materialized_views) r "
                + "JOIN rw_catalog.rw_schemas s ON s.id = r.schema_id "
                + "JOIN rw_catalog.rw_columns c ON c.relation_id = r.id "
                + "WHERE s.name = 'public' AND NOT c.is_hidden ORDER BY r.name, c.position";
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                kinds.put(rs.getString(1), RelationKind.valueOf(rs.getString(5)));
                tables.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(new RisingWaveColumn(
                        rs.getString(2), RisingWaveType.fromSql(rs.getString(3)), rs.getBoolean(4)));
            }
        }
        List<RisingWaveTable> result = new ArrayList<>();
        tables.forEach((name, columns) -> result.add(new RisingWaveTable(name, columns, null, kinds.get(name))));
        return new RisingWaveSchema(result);
    }
}
