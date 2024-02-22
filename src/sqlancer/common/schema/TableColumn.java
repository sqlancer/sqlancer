package sqlancer.common.schema;

public interface TableColumn<U> extends Comparable<TableColumn<U>> {
    String getName();

    String getFullQualifiedName();

    void setTable(Table<U> table);

    Table<U> getTable();

    boolean isPrimaryKey();

    U getType();

    @Override
    String toString();

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

    @Override
    default int compareTo(TableColumn<U> o) {
        return getName().compareTo(o.getName());
    }

}
