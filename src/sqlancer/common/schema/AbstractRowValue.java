package sqlancer.common.schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractRowValue<U, O> implements RowValue<U, O> {

    private final TableGroup<U> tables;
    private final Map<TableColumn<U>, O> values;

    protected AbstractRowValue(TableGroup<U> tables, Map<TableColumn<U>, O> values) {
        this.tables = tables;
        this.values = values;
    }

    public TableGroup<U> getTable() {
        return tables;
    }

    public Map<TableColumn<U>, O> getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (TableColumn<U> c : tables.getColumns()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(values.get(c));
        }
        return sb.toString();
    }

    public String getRowValuesAsString() {
        List<TableColumn<U>> columnsToCheck = tables.getColumns();
        return getRowValuesAsString(columnsToCheck);
    }

    public String getRowValuesAsString(List<TableColumn<U>> columnsToCheck) {
        StringBuilder sb = new StringBuilder();
        Map<TableColumn<U>, O> expectedValues = getValues();
        for (int i = 0; i < columnsToCheck.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            O expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
            sb.append(expectedColumnValue);
        }
        return sb.toString();
    }

    public String asStringGroupedByTables() {
        StringBuilder sb = new StringBuilder();
        List<TableColumn<U>> columnList = getValues().keySet().stream().collect(Collectors.toList());
        List<Table<U>> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
                .collect(Collectors.toList());
        for (int j = 0; j < tableList.size(); j++) {
            if (j != 0) {
                sb.append("\n");
            }
            Table<U> t = tableList.get(j);
            sb.append("-- ").append(t.getName()).append("\n");
            List<TableColumn<U>> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
                    .collect(Collectors.toList());
            for (int i = 0; i < columnsForTable.size(); i++) {
                if (i != 0) {
                    sb.append("\n");
                }
                sb.append("--\t");
                sb.append(columnsForTable.get(i));
                sb.append("=");
                sb.append(getValues().get(columnsForTable.get(i)));
            }
        }
        return sb.toString();
    }

}
