package sqlancer.schema;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.Randomly;

public class AbstractTable<COL extends AbstractTableColumn<?, ?>, I extends TableIndex>
		implements Comparable<AbstractTable<?, ?>> {

	private final String name;
	private final List<COL> columns;
	private final List<I> indexes;
	private final boolean isView;

	public AbstractTable(String name, List<COL> columns, List<I> indexes, boolean isView) {
		this.name = name;
		this.indexes = indexes;
		this.isView = isView;
		this.columns = Collections.unmodifiableList(columns);
	}

	public String getName() {
		return name;
	}

	@Override
	public int compareTo(AbstractTable<?, ?> o) {
		return o.getName().compareTo(getName());
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(getName() + "\n");
		for (COL c : columns) {
			sb.append("\t" + c + "\n");
		}
		return sb.toString();
	}

	public List<I> getIndexes() {
		return indexes;
	}

	public List<COL> getColumns() {
		return columns;
	}

	public String getColumnsAsString() {
		return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
	}

	public String getColumnsAsString(Function<COL, String> function) {
		return columns.stream().map(function).collect(Collectors.joining(", "));
	}

	public COL getRandomColumn() {
		return Randomly.fromList(columns);
	}

	public boolean hasIndexes() {
		return !indexes.isEmpty();
	}

	public TableIndex getRandomIndex() {
		return Randomly.fromList(indexes);
	}

	public List<COL> getRandomNonEmptyColumnSubset() {
		return Randomly.nonEmptySubset(getColumns());
	}

	public List<COL> getRandomNonEmptyColumnSubset(int size) {
		return Randomly.nonEmptySubset(getColumns(), size);
	}

	public boolean isView() {
		return isView;
	}

}
