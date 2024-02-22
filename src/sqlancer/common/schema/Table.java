package sqlancer.common.schema;

import java.util.List;
import java.util.function.Predicate;

public interface Table<U> extends Comparable<Table<U>> {

    String getName();

    List<TableIndex<U>> getIndexes();

    List<TableColumn<U>> getColumns();

    String getColumnsAsString();

    TableColumn<U> getRandomColumn();

    TableColumn<U> getRandomColumnOrBailout(Predicate<TableColumn<U>> predicate);

    boolean hasIndexes();

    TableIndex<U> getRandomIndex();

    List<TableColumn<U>> getRandomNonEmptyColumnSubset();

    List<TableColumn<U>> getRandomNonEmptyColumnSubset(int size);

    boolean isView();

    String getFreeColumnName();

    void recomputeCount();

    String getFullyQualifiedColumnName(TableColumn<U> column);

    // long getNrRows(G globalState);

    @Override
    String toString();

    @Override
    default int compareTo(Table<U> o) {
        return o.getName().compareTo(getName());
    }

}
