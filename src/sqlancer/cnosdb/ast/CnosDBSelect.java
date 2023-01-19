package sqlancer.cnosdb.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;
import sqlancer.common.ast.SelectBase;

public class CnosDBSelect extends SelectBase<CnosDBExpression> implements CnosDBExpression {

    private SelectType selectOption = SelectType.ALL;
    private List<CnosDBJoin> joinClauses = Collections.emptyList();
    private CnosDBExpression distinctOnClause;

    public void setSelectType(SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    public SelectType getSelectOption() {
        return selectOption;
    }

    public void setSelectOption(SelectType fromOptions) {
        this.selectOption = fromOptions;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return null;
    }

    public List<CnosDBJoin> getJoinClauses() {
        return joinClauses;
    }

    public void setJoinClauses(List<CnosDBJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    public CnosDBExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setDistinctOnClause(CnosDBExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
    }

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public static class CnosDBFromTable implements CnosDBExpression {
        private final CnosDBTable t;

        public CnosDBFromTable(CnosDBTable t) {
            this.t = t;
        }

        public CnosDBTable getTable() {
            return t;
        }

        @Override
        public CnosDBDataType getExpressionType() {
            return null;
        }
    }

    public static class CnosDBSubquery implements CnosDBExpression {
        private final CnosDBSelect s;
        private final String name;

        public CnosDBSubquery(CnosDBSelect s, String name) {
            this.s = s;
            this.name = name;
        }

        public CnosDBSelect getSelect() {
            return s;
        }

        public String getName() {
            return name;
        }

        @Override
        public CnosDBDataType getExpressionType() {
            return null;
        }
    }

}
