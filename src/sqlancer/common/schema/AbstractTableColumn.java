package sqlancer.common.schema;

public abstract class AbstractTableColumn<U> implements TableColumn<U> {

    private final String name;
    private final U type;
    private Table<U> table;

    public AbstractTableColumn(String name, Table<U> table, U type) {
        this.name = name;
        this.table = table;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getFullQualifiedName() {
        if (table == null) {
            return getName();
        } else {
            return table.getName() + "." + getName();
        }
    }

    public void setTable(Table<U> table) {
        this.table = table;
    }

    public Table<U> getTable() {
        return table;
    }

    public U getType() {
        return type;
    }

    @Override
    public String toString() {
        if (table == null) {
            return String.format("%s: %s", getName(), getType());
        } else {
            return String.format("%s.%s: %s", table.getName(), getName(), getType());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbstractTableColumn)) {
            return false;
        } else {
            @SuppressWarnings("unchecked")
            TableColumn<U> c = (TableColumn<U>) obj;
            if (c.getTable() == null) {
                return getName().equals(c.getName());
            }
            return table.getName().contentEquals(c.getTable().getName()) && getName().equals(c.getName());
        }
    }

    @Override
    public int hashCode() {
        return getName().hashCode() + 11 * getType().hashCode();
    }

    @Override
    public int compareTo(TableColumn<U> o) {
        if (o.getTable().equals(this.getTable())) {
            return getName().compareTo(o.getName());
        } else {
            return o.getTable().compareTo(getTable());
        }
    }

}
