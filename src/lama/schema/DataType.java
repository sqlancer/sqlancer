package lama.schema;

public class DataType {
	
	private final PrimitiveDataType primitiveType;
	
	public DataType(PrimitiveDataType primitiveType) {
		this.primitiveType = primitiveType;
	}

	public PrimitiveDataType getPrimitiveType() {
		return primitiveType;
	}
}
