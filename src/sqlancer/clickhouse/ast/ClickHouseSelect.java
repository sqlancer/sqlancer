package sqlancer.clickhouse.ast;

import java.util.Collections;
import java.util.List;

public class ClickHouseSelect extends ClickHouseExpression {

    private ClickHouseSelect.SelectType fromOptions = ClickHouseSelect.SelectType.ALL;
    private List<ClickHouseExpression> fromList = Collections.emptyList();
    private ClickHouseExpression whereClause;
    private List<ClickHouseExpression> groupByClause = Collections.emptyList();
    private ClickHouseExpression limitClause;
    private List<ClickHouseExpression> orderByClause = Collections.emptyList();
    private ClickHouseExpression offsetClause;
    private List<ClickHouseExpression> fetchColumns = Collections.emptyList();
    private List<ClickHouseExpression.ClickHouseJoin> joinStatements = Collections.emptyList();
    private ClickHouseExpression havingClause;

    public enum SelectType {
        DISTINCT, ALL;
    }

    public void setSelectType(ClickHouseSelect.SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public void setFromTables(List<ClickHouseExpression> fromTables) {
        this.setFromList(fromTables);
    }

    public ClickHouseSelect.SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(ClickHouseSelect.SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    public List<ClickHouseExpression> getFromList() {
        return fromList;
    }

    public void setFromList(List<ClickHouseExpression> fromList) {
        this.fromList = fromList;
    }

    public ClickHouseExpression getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(ClickHouseExpression whereClause) {
        this.whereClause = whereClause;
    }

    public void setGroupByClause(List<ClickHouseExpression> groupByClause) {
        this.groupByClause = groupByClause;
    }

    public List<ClickHouseExpression> getGroupByClause() {
        return groupByClause;
    }

    public void setLimitClause(ClickHouseExpression limitClause) {
        this.limitClause = limitClause;
    }

    public ClickHouseExpression getLimitClause() {
        return limitClause;
    }

    public List<ClickHouseExpression> getOrderByClause() {
        return orderByClause;
    }

    public void setOrderByExpressions(List<ClickHouseExpression> orderBy) {
        this.orderByClause = orderBy;
    }

    public void setOffsetClause(ClickHouseExpression offsetClause) {
        this.offsetClause = offsetClause;
    }

    public ClickHouseExpression getOffsetClause() {
        return offsetClause;
    }

    public void setFetchColumns(List<ClickHouseExpression> fetchColumns) {
        this.fetchColumns = fetchColumns;
    }

    public List<ClickHouseExpression> getFetchColumns() {
        return fetchColumns;
    }

    public void setJoinClauses(List<ClickHouseExpression.ClickHouseJoin> joinStatements) {
        this.joinStatements = joinStatements;
    }

    public List<ClickHouseExpression.ClickHouseJoin> getJoinClauses() {
        return joinStatements;
    }

    public void setHavingClause(ClickHouseExpression havingClause) {
        this.havingClause = havingClause;
    }

    public ClickHouseExpression getHavingClause() {
        assert orderByClause != null;
        return havingClause;
    }
}
