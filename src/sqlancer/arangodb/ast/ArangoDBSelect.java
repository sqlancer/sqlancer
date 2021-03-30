package sqlancer.arangodb.ast;

import java.util.List;

import sqlancer.arangodb.ArangoDBSchema;
import sqlancer.common.ast.newast.Node;

public class ArangoDBSelect<E> implements Node<E> {
    private List<ArangoDBSchema.ArangoDBColumn> fromColumns;
    private List<ArangoDBSchema.ArangoDBColumn> projectionColumns;
    private boolean hasFilter;
    private Node<E> filterClause;
    private boolean hasComputed;
    private List<Node<E>> computedClause;

    public List<ArangoDBSchema.ArangoDBColumn> getFromColumns() {
        if (fromColumns == null || fromColumns.isEmpty()) {
            throw new IllegalStateException();
        }
        return fromColumns;
    }

    public void setFromColumns(List<ArangoDBSchema.ArangoDBColumn> fromColumns) {
        if (fromColumns == null || fromColumns.isEmpty()) {
            throw new IllegalStateException();
        }
        this.fromColumns = fromColumns;
    }

    public List<ArangoDBSchema.ArangoDBColumn> getProjectionColumns() {
        if (projectionColumns == null) {
            throw new IllegalStateException();
        }
        return projectionColumns;
    }

    public void setProjectionColumns(List<ArangoDBSchema.ArangoDBColumn> projectionColumns) {
        if (projectionColumns == null) {
            throw new IllegalStateException();
        }
        this.projectionColumns = projectionColumns;
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

    public void setComputedClause(List<Node<E>> computedColumns) {
        if (computedColumns == null || computedColumns.isEmpty()) {
            hasComputed = false;
            this.computedClause = null;
            return;
        }
        hasComputed = true;
        this.computedClause = computedColumns;
    }

    public List<Node<E>> getComputedClause() {
        return computedClause;
    }

    public boolean hasComputed() {
        return hasComputed;
    }
}
