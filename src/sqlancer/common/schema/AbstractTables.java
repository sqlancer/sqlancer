package sqlancer.common.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AbstractTables<T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    private final List<T> tables;
    private final List<C> columns;

    public AbstractTables(List<T> tables) {
        this.tables = tables;
        columns = new ArrayList<>();
        for (T t : tables) {
            columns.addAll(t.getColumns());
        }
    }

    public String tableNamesAsString() {
        return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
    }

    public List<T> getTables() {
        return tables;
    }

    public List<C> getColumns() {
        return columns;
    }

    public String columnNamesAsString(Function<C, String> function) {
        return getColumns().stream().map(function).collect(Collectors.joining(", "));
    }


    public void addTable(T table) {
        if (!this.tables.contains(table)) {
            this.tables.add(table);
            columns.addAll(table.getColumns());
        }
    }

    public void removeTable(T table) {
        if (this.tables.contains(table)) {
            this.tables.remove(table);
            for (C c : table.getColumns()) {
                columns.remove(c);
            }
        }
    }

    public boolean isContained(T table) {
        return this.tables.contains(table);
    }

    public int getSize() {
        return this.tables.size();
    }
}
