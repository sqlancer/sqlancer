package lama.schema;

public class DataType {
	
	private final SQLite3DataType primitiveType;
	
	public DataType(SQLite3DataType primitiveType) {
		this.primitiveType = primitiveType;
	}

	public SQLite3DataType getPrimitiveType() {
		return primitiveType;
	}
}
