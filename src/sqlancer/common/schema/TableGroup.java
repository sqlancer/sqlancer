package sqlancer.common.schema;

import java.util.List;
import java.util.function.Function;

public interface TableGroup<U> {
    String tableNamesAsString();

    List<? extends Table<U>> getTables();

    List<? extends TableColumn<U>> getColumns();

    String columnNamesAsString(Function<TableColumn<U>, String> function);
}
