package sqlancer.mongodb.ast;

import java.util.List;

import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBSelect<E> implements Node<E> {

    private final String mainTableName;
    private final MongoDBColumnTestReference joinColumn;
    List<MongoDBColumnTestReference> projectionColumns;
    List<MongoDBColumnTestReference> lookupList;
    boolean hasFilter;
    Node<E> filterClause;

    public MongoDBSelect(String mainTableName, MongoDBColumnTestReference joinColumn) {
        this.mainTableName = mainTableName;
        this.joinColumn = joinColumn;
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public MongoDBColumnTestReference getJoinColumn() {
        return joinColumn;
    }

    public void setProjectionList(List<MongoDBColumnTestReference> fetchColumns) {
        if (fetchColumns == null || fetchColumns.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.projectionColumns = fetchColumns;
    }

    public List<MongoDBColumnTestReference> getProjectionList() {
        if (projectionColumns == null) {
            throw new IllegalStateException();
        }
        return projectionColumns;
    }

    public void setLookupList(List<MongoDBColumnTestReference> lookupList) {
        if (lookupList == null || lookupList.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.lookupList = lookupList;
    }

    public List<MongoDBColumnTestReference> getLookupList() {
        if (lookupList == null) {
            throw new IllegalStateException();
        }
        return lookupList;
    }

    public void setFilterClause(Node<E> filterClause) {
        if (filterClause == null) {
            hasFilter = false;
            this.filterClause = null;
            return;
        }
        hasFilter = true;
        this.filterClause = filterClause;
    }

    public Node<E> getFilterClause() {
        return filterClause;
    }

    public boolean hasFilter() {
        return hasFilter;
    }

}
