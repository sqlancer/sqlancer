package sqlancer.mariadb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

public class MariaDBSelectStatement extends SelectBase<MariaDBExpression>
        implements MariaDBExpression, Select<MariaDBJoin, MariaDBExpression, MariaDBTable, MariaDBColumn> {

    public enum MariaDBSelectType {
        ALL, DISTINCT, DISTINCTROW;
    }

    private List<MariaDBExpression> groupBys = new ArrayList<>();
    private List<MariaDBExpression> columns = new ArrayList<>();
    private List<JoinBase<MariaDBExpression>> joinClauses = new ArrayList<>();
    private MariaDBSelectType selectType = MariaDBSelectType.ALL;
    private MariaDBExpression whereCondition;

    @Override
    public void setGroupByClause(List<MariaDBExpression> groupBys) {
        this.groupBys = groupBys;
    }

    @Override
    public void setFetchColumns(List<MariaDBExpression> columns) {
        this.columns = columns;

    }

    public void setSelectType(MariaDBSelectType selectType) {
        this.selectType = selectType;
    }

    @Override
    public void setWhereClause(MariaDBExpression whereCondition) {
        this.whereCondition = whereCondition;
    }

    public List<MariaDBExpression> getColumns() {
        return columns;
    }

    public List<MariaDBExpression> getGroupBys() {
        return groupBys;
    }

    public MariaDBSelectType getSelectType() {
        return selectType;
    }

    public MariaDBExpression getWhereCondition() {
        return whereCondition;
    }

    @Override
    public List<JoinBase<MariaDBExpression>> getJoinClauses() {
        return joinClauses;
    }

    @Override
    public void setJoinClauses(List<JoinBase<MariaDBExpression>> joinClauses) {
        this.joinClauses = joinClauses;
    }

    @Override
    public String asString() {
        return MariaDBVisitor.asString(this);
    }
}
