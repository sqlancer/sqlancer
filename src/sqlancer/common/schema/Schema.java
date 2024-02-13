package sqlancer.common.schema;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public interface Schema<U> {
    String toString();

    Table<U> getRandomTable();

    Table<U> getRandomTableOrBailout();

    Table<U> getRandomTable(Predicate<Table<U>> predicate);

    Table<U> getRandomTableOrBailout(Function<Table<U>, Boolean> f);

    List<Table<U>> getDatabaseTables();

    List<Table<U>> getTables(Predicate<Table<U>> predicate);

    List<Table<U>> getDatabaseTablesRandomSubsetNotEmpty();

    Table<U> getDatabaseTable(String name);

    List<Table<U>> getViews();

    List<Table<U>> getDatabaseTablesWithoutViews();

    Table<U> getRandomViewOrBailout();

    Table<U> getRandomTableNoViewOrBailout();

    String getFreeIndexName();

    String getFreeTableName();

    String getFreeViewName();

    boolean containsTableWithZeroRows(G globalState);

    TableGroup<U> getRandomTableNonEmptyTables();


}
