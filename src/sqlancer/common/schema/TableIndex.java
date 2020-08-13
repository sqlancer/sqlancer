package sqlancer.common.schema;

public class TableIndex {

    private final String indexName;

    protected TableIndex(String indexName) {
        this.indexName = indexName;
    }

    public static TableIndex create(String indexName) {
        return new TableIndex(indexName);
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public String toString() {
        return indexName;
    }

}
