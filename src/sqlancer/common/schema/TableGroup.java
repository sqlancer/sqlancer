package sqlancer.common.schema;

import java.util.List;
import java.util.function.Function;

public interface TableGroup<U> {
    String tableNamesAsString();

    List<Table<U>> getTables();

    List<TableColumn<U>> getColumns();

    String columnNamesAsString(Function<TableColumn<U>, String> function);
}
