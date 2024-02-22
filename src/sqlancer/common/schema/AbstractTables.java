package sqlancer.common.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AbstractTables<U> implements TableGroup<U> {

    private final List<? extends Table<U>> tables;
    private final List<TableColumn<U>> columns;

    public AbstractTables(List<? extends Table<U>> tables) {
        this.tables = tables;
        columns = new ArrayList<>();
        for (Table<U> t : tables) {
            columns.addAll(t.getColumns());
        }
    }

    public String tableNamesAsString() {
        return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
    }

    public List<? extends Table<U>> getTables() {
        return tables;
    }

    public List<? extends TableColumn<U>> getColumns() {
        return columns;
    }

    public String columnNamesAsString(Function<TableColumn<U>, String> function) {
        return getColumns().stream().map(function).collect(Collectors.joining(", "));
    }

}
