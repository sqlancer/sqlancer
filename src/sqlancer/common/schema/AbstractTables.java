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

}
