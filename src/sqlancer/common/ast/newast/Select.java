package sqlancer.common.ast.newast;

import java.util.List;

import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;

public interface Select<J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>>
        extends Expression<C> {

    void setFromTables(List<E> fromTables);

    List<E> getFromList();

    void setFromList(List<E> fromList);

    Expression<C> getWhereClause();

    void setWhereClause(E whereClause);

    void setGroupByClause(List<E> groupByClause);

    List<E> getGroupByClause();

    void setLimitClause(E limitClause);

    Expression<C> getLimitClause();

    List<E> getOrderByClause();

    void setOrderByExpressions(List<E> orderBy);

    void setOffsetClause(E offsetClause);

    Expression<C> getOffsetClause();

    void setFetchColumns(List<E> fetchColumns);

    List<E> getFetchColumns();

    void setJoinClauses(List<J> joinStatements);

    List<J> getJoinClauses();

    void setHavingClause(E havingClause);

    Expression<C> getHavingClause();

    String asString();
}
