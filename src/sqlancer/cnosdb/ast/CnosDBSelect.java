package sqlancer.cnosdb.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;

public class CnosDBSelect extends SelectBase<CnosDBExpression>
        implements CnosDBExpression {

    private List<JoinBase<CnosDBExpression>> joinClauses = Collections.emptyList();
    private CnosDBExpression distinctOnClause;

    public void setSelectType(SelectBase.SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return null;
    }


    @Override
    public List<JoinBase<CnosDBExpression>> getJoinClauses() {
        return joinClauses;
    }

    public void setJoinClauses(List<? extends JoinBase<CnosDBExpression>> joinStatements) {
        this.joinClauses = (List<JoinBase<CnosDBExpression>>) joinStatements;
    }

    public CnosDBExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setDistinctOnClause(CnosDBExpression distinctOnClause) {
        if (getSelectOption() != SelectBase.SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
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
