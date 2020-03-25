package sqlancer.schema;

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;

public class AbstractTableColumn<T extends AbstractTable<?, ?>, U>  implements Comparable<AbstractTableColumn<T, U>>  {

	private final String name;
	private final U type;
	private T table;
	
	public AbstractTableColumn(String name, T table, U type) {
		this.name = name;
		this.table = table;
		this.type = type;
	}
	
	public String getName() {
		return name;
	}
	
	public String getFullQualifiedName() {
		return table.getName() + "." + getName();
	}
	
	public void setTable(T table) {
		this.table = table;
	}
	
	public T getTable() {
		return table;
	}
	
	public U getType() {
		return type;
	}
	
	@Override
	public String toString() {
		return String.format("%s.%s: %s", table.getName(), getName(), getType());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof AbstractTableColumn)) {
			return false;
		} else {
			CockroachDBColumn c = (CockroachDBColumn) obj;
			return table.getName().contentEquals(getName()) && getName().equals(c.getName());
		}
	}
	
	@Override
	public int hashCode() {
		return getName().hashCode() + 11 * getType().hashCode();
	}
	

	@Override
	public int compareTo(AbstractTableColumn<T, U> o) {
		if (o.getTable().equals(this.getTable())) {
			return getName().compareTo(o.getName());
		} else {
			return o.getTable().compareTo(getTable());
		}
	}

	
}
