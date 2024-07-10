package sqlancer.mariadb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.ast.SelectBase;

public class MariaDBSelectStatement extends SelectBase<MariaDBExpression> implements MariaDBExpression {

    public enum MariaDBSelectType {
        ALL, DISTINCT, DISTINCTROW;
    }

    private List<MariaDBExpression> groupBys = new ArrayList<>();
    private List<MariaDBExpression> columns = new ArrayList<>();
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

}
