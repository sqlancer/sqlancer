package sqlancer.common.schema;

public interface RelationalTable<U> extends Table<U> {
    boolean isVirtual();
}
