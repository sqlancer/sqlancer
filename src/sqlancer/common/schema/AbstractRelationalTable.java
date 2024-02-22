package sqlancer.common.schema;

import java.util.List;

public abstract class AbstractRelationalTable<U> extends AbstractTable<U> implements RelationalTable<U> {

    public AbstractRelationalTable(String name, List<? extends TableColumn<U>> columns,
            List<TableIndex<U>> indexes, boolean isView) {
        super(name, columns, indexes, isView);
    }

    // @Override
    // public long getNrRows(G globalState) {
    // if (rowCount == NO_ROW_COUNT_AVAILABLE) {
    // SQLQueryAdapter q = new SQLQueryAdapter("SELECT COUNT(*) FROM " + name);
    // try (SQLancerResultSet query = q.executeAndGet(globalState)) {
    // if (query == null) {
    // throw new IgnoreMeException();
    // }
    // query.next();
    // rowCount = query.getLong(1);
    // return rowCount;
    // } catch (Throwable t) {
    // // an exception might be expected, for example, when invalid view is created
    // throw new IgnoreMeException();
    // }
    // } else {
    // return rowCount;
    // }
    // }
    //
}
