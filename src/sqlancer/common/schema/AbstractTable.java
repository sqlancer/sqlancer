package sqlancer.common.schema;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;

public abstract class AbstractTable<U> implements Table<U> {

    protected static final int NO_ROW_COUNT_AVAILABLE = -1;
    protected final String name;
    private final List<? extends TableColumn<U>> columns;
    private final List<? extends TableIndex<U>> indexes;
    private final boolean isView;
    protected long rowCount = NO_ROW_COUNT_AVAILABLE;

    protected AbstractTable(String name, List<? extends TableColumn<U>> columns, List<? extends TableIndex<U>> indexes,
            boolean isView) {
        this.name = name;
        this.indexes = indexes;
        this.isView = isView;
        this.columns = Collections.unmodifiableList(columns);
    }

    public String getName() {
        return name;
    }

    @Override
    public int compareTo(Table<U> o) {
        return o.getName().compareTo(getName());
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append("\n");
        for (TableColumn<U> c : columns) {
            sb.append("\t").append(c).append("\n");
        }
        return sb.toString();
    }

    public List<? extends TableIndex<U>> getIndexes() {
        return indexes;
    }

    public List<? extends TableColumn<U>> getColumns() {
        return columns;
    }

    public String getColumnsAsString() {
        return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
    }

    public TableColumn<U> getRandomColumn() {
        return Randomly.fromList(columns);
    }

    public TableColumn<U> getRandomColumnOrBailout(Predicate<TableColumn<U>> predicate) {
        List<TableColumn<U>> relevantColumns = columns.stream().filter(predicate).collect(Collectors.toList());
        if (relevantColumns.isEmpty()) {
            throw new IgnoreMeException();
        }

        return Randomly.fromList(relevantColumns);
    }

    public boolean hasIndexes() {
        return !indexes.isEmpty();
    }

    public TableIndex<U> getRandomIndex() {
        return Randomly.fromList(indexes);
    }

    public List<? extends TableColumn<U>> getRandomNonEmptyColumnSubset() {
        return Randomly.nonEmptySubset(getColumns());
    }

    public List<? extends TableColumn<U>> getRandomNonEmptyColumnSubset(int size) {
        return Randomly.nonEmptySubset(getColumns(), size);
    }

    public boolean isView() {
        return isView;
    }

    public String getFreeColumnName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String columnName = String.format("c%d", i++);
            if (columns.stream().noneMatch(t -> t.getName().contentEquals(columnName))) {
                return columnName;
            }
        } while (true);

    }

    public void recomputeCount() {
        rowCount = NO_ROW_COUNT_AVAILABLE;
    }

    // public abstract long getNrRows(G globalState);
}
