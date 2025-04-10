package sqlancer.sqlite3.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.newast.Select;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3Select extends SQLite3Expression
        implements Select<Join, SQLite3Expression, SQLite3Table, SQLite3Column> {

    private SelectType fromOptions = SelectType.ALL;
    private List<SQLite3Expression> fromList = Collections.emptyList();
    private SQLite3Expression whereClause;
    private List<SQLite3Expression> groupByClause = Collections.emptyList();
    private SQLite3Expression limitClause;
    private List<SQLite3Expression> orderByClause = Collections.emptyList();
    private SQLite3Expression offsetClause;
    private List<SQLite3Expression> fetchColumns = Collections.emptyList();
    private List<Join> joinStatements = Collections.emptyList();
    private SQLite3Expression havingClause;
    private SQLite3WithClause withClause = null;

    public SQLite3Select() {
    }

    public SQLite3Select(SQLite3Select other) {
        fromOptions = other.fromOptions;
        fromList = new ArrayList<>(other.fromList);
        whereClause = other.whereClause;
        groupByClause = other.groupByClause;
        limitClause = other.limitClause;
        orderByClause = new ArrayList<>(other.orderByClause);
        offsetClause = other.offsetClause;
        fetchColumns = new ArrayList<>(other.fetchColumns);
        joinStatements = new ArrayList<>();
        for (Join j : other.joinStatements) {
            joinStatements.add(new Join(j));
        }
        havingClause = other.havingClause;
        withClause = other.withClause;
    }

    public enum SelectType {
        DISTINCT, ALL;
    }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    @Override
    public List<SQLite3Expression> getFromList() {
        return fromList;
    }

    @Override
    public void setFromList(List<SQLite3Expression> fromList) {
        this.fromList = fromList;
    }

    @Override
    public SQLite3Expression getWhereClause() {
        return whereClause;
    }

    @Override
    public void setWhereClause(SQLite3Expression whereClause) {
        this.whereClause = whereClause;
    }

    @Override
    public void setGroupByClause(List<SQLite3Expression> groupByClause) {
        this.groupByClause = groupByClause;
    }

    @Override
    public List<SQLite3Expression> getGroupByClause() {
        return groupByClause;
    }

    @Override
    public void setLimitClause(SQLite3Expression limitClause) {
        this.limitClause = limitClause;
    }

    @Override
    public SQLite3Expression getLimitClause() {
        return limitClause;
    }

    @Override
    public List<SQLite3Expression> getOrderByClauses() {
        return orderByClause;
    }

    @Override
    public void setOrderByClauses(List<SQLite3Expression> orderBy) {
        this.orderByClause = orderBy;
    }

    @Override
    public void setOffsetClause(SQLite3Expression offsetClause) {
        this.offsetClause = offsetClause;
    }

    @Override
    public SQLite3Expression getOffsetClause() {
        return offsetClause;
    }

    @Override
    public void setFetchColumns(List<SQLite3Expression> fetchColumns) {
        this.fetchColumns = fetchColumns;
    }

    @Override
    public List<SQLite3Expression> getFetchColumns() {
        return fetchColumns;
    }

    @Override
    public void setJoinClauses(List<Join> joinStatements) {
        this.joinStatements = joinStatements;
    }

    @Override
    public List<Join> getJoinClauses() {
        return joinStatements;
    }

    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
        // TODO implement?
        return null;
    }

    @Override
    public void setHavingClause(SQLite3Expression havingClause) {
        this.havingClause = havingClause;
    }

    @Override
    public SQLite3Expression getHavingClause() {
        assert orderByClause != null;
        return havingClause;
    }

    @Override
    public String asString() {
        return SQLite3Visitor.asString(this);
    }

    public void setWithClause(SQLite3WithClause withClause) {
        this.withClause = withClause;
    }

    public void updateWithClauseRight(SQLite3Expression withClauseRight) {
        this.withClause.updateRight(withClauseRight);
    }

    public SQLite3Expression getWithClause() {
        return this.withClause;
    }

    // This method is used in CODDTest to test subquery by replacing a table name
    // in the SELECT clause with a derived table expression. 
    public void replaceFromTable(String tableName, SQLite3Expression newFromExpression) {
        int replaceIdx = -1;
        for (int i = 0; i < fromList.size(); ++i) {
            SQLite3Expression f = fromList.get(i);
            if (f instanceof SQLite3TableReference) {
                SQLite3TableReference tableRef = (SQLite3TableReference) f;
                if (tableRef.getTable().getName() == tableName) {
                    replaceIdx = i;
                }
            }
        }
        if (replaceIdx == -1) {
            throw new IgnoreMeException();
        }
        fromList.set(replaceIdx, newFromExpression);
    }
}
