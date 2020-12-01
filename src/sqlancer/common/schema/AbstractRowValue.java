package sqlancer.common.schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractRowValue<T extends AbstractTables<?, C>, C extends AbstractTableColumn<?, ?>, O> {

    private final T tables;
    private final Map<C, O> values;

    protected AbstractRowValue(T tables, Map<C, O> values) {
        this.tables = tables;
        this.values = values;
    }

    public T getTable() {
        return tables;
    }

    public Map<C, O> getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (C c : tables.getColumns()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(values.get(c));
        }
        return sb.toString();
    }

    public String getRowValuesAsString() {
        List<C> columnsToCheck = tables.getColumns();
        return getRowValuesAsString(columnsToCheck);
    }

    public String getRowValuesAsString(List<C> columnsToCheck) {
        StringBuilder sb = new StringBuilder();
        Map<C, O> expectedValues = getValues();
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
        List<C> columnList = getValues().keySet().stream().collect(Collectors.toList());
        List<AbstractTable<?, ?, ?>> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
                .collect(Collectors.toList());
        for (int j = 0; j < tableList.size(); j++) {
            if (j != 0) {
                sb.append("\n");
            }
            AbstractTable<?, ?, ?> t = tableList.get(j);
            sb.append("-- " + t.getName() + "\n");
            List<C> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
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
