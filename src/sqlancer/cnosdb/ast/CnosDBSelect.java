package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;
import sqlancer.common.ast.SelectBase;

import java.util.Collections;
import java.util.List;

public class CnosDBSelect extends SelectBase<CnosDBExpression> implements CnosDBExpression {

    private SelectType selectOption = SelectType.ALL;
    private List<CnosDBJoin> joinClauses = Collections.emptyList();
    private CnosDBExpression distinctOnClause;

    public static class CnosDBFromTable implements CnosDBExpression {
        private final CnosDBTable t;

        public CnosDBFromTable(CnosDBTable t, boolean only) {
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

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public void setSelectType(SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    public void setDistinctOnClause(CnosDBExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
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

    public void setJoinClauses(List<CnosDBJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    public List<CnosDBJoin> getJoinClauses() {
        return joinClauses;
    }

    public CnosDBExpression getDistinctOnClause() {
        return distinctOnClause;
    }

}
