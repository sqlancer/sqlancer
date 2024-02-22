package sqlancer.common.schema;

public class TableIndex<U> {

    private final String indexName;

    protected TableIndex(String indexName) {
        this.indexName = indexName;
    }

    public static <U> TableIndex<U> create(String indexName) {
        return new TableIndex<U>(indexName);
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public String toString() {
        return indexName;
    }

}
