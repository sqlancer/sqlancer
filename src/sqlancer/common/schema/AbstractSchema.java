package sqlancer.common.schema;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;

public class AbstractSchema<U> implements Schema<U> {

    private final List<Table<U>> databaseTables;

    public AbstractSchema(List<Table<U>> databaseTables) {
        this.databaseTables = Collections.unmodifiableList(databaseTables);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Table<U> t : getDatabaseTables()) {
            sb.append(t);
            sb.append("\n");
        }
        return sb.toString();
    }

    public Table<U> getRandomTable() {
        return Randomly.fromList(getDatabaseTables());
    }

    public Table<U> getRandomTableOrBailout() {
        if (databaseTables.isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getDatabaseTables());
        }
    }

    public Table<U> getRandomTable(Predicate<Table<U>> predicate) {
        return Randomly.fromList(getDatabaseTables().stream().filter(predicate).collect(Collectors.toList()));
    }

    public Table<U> getRandomTableOrBailout(Function<Table<U>, Boolean> f) {
        List<Table<U>> relevantTables = databaseTables.stream().filter(f::apply).collect(Collectors.toList());
        if (relevantTables.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(relevantTables);
    }

    public List<Table<U>> getDatabaseTables() {
        return databaseTables;
    }

    public List<Table<U>> getTables(Predicate<Table<U>> predicate) {
        return databaseTables.stream().filter(predicate).collect(Collectors.toList());
    }

    public List<Table<U>> getDatabaseTablesRandomSubsetNotEmpty() {
        return Randomly.nonEmptySubset(databaseTables);
    }

    public Table<U> getDatabaseTable(String name) {
        return databaseTables.stream().filter(t -> t.getName().equals(name)).findAny().orElse(null);
    }

    public List<Table<U>> getViews() {
        return databaseTables.stream().filter(t -> t.isView()).collect(Collectors.toList());
    }

    public List<Table<U>> getDatabaseTablesWithoutViews() {
        return databaseTables.stream().filter(t -> !t.isView()).collect(Collectors.toList());
    }

    public Table<U> getRandomViewOrBailout() {
        if (getViews().isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getViews());
        }
    }

    public Table<U> getRandomTableNoViewOrBailout() {
        List<Table<U>> databaseTablesWithoutViews = getDatabaseTablesWithoutViews();
        if (databaseTablesWithoutViews.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(databaseTablesWithoutViews);
    }

    public String getFreeIndexName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String indexName = String.format("i%d", i++);
            boolean indexNameFound = false;
            for (Table<U> table : databaseTables) {
                if (table.getIndexes().stream().anyMatch(ind -> ind.getIndexName().contentEquals(indexName))) {
                    indexNameFound = true;
                    break;
                }
            }
            if (!indexNameFound) {
                return indexName;
            }
        } while (true);
    }

    public String getFreeTableName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("t%d", i++);
            if (databaseTables.stream().noneMatch(t -> t.getName().equalsIgnoreCase(tableName))) {
                return tableName;
            }
        } while (true);

    }

    public String getFreeViewName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("v%d", i++);
            if (databaseTables.stream().noneMatch(t -> t.getName().contentEquals(tableName))) {
                return tableName;
            }
        } while (true);
    }

    // public boolean containsTableWithZeroRows(G globalState) {
    // return databaseTables.stream().anyMatch(t -> t.getNrRows(globalState) == 0);
    // }

    public TableGroup<U> getRandomTableNonEmptyTables() {
        throw new IgnoreMeException();
    }

}
