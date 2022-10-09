package sqlancer.yugabyte.ysql.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public class YSQLSelect extends SelectBase<YSQLExpression> implements YSQLExpression {

    private SelectType selectOption = SelectType.ALL;
    private List<YSQLJoin> joinClauses = Collections.emptyList();
    private YSQLExpression distinctOnClause;
    private ForClause forClause;

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
    public YSQLDataType getExpressionType() {
        return null;
    }

    public List<YSQLJoin> getJoinClauses() {
        return joinClauses;
    }

    public void setJoinClauses(List<YSQLJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    public YSQLExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setDistinctOnClause(YSQLExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
    }

    public ForClause getForClause() {
        return forClause;
    }

    public void setForClause(ForClause forClause) {
        this.forClause = forClause;
    }

    public enum ForClause {
        UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE");

        private final String textRepresentation;

        ForClause(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static ForClause getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public static class YSQLFromTable implements YSQLExpression {
        private final YSQLTable t;
        private final boolean only;

        public YSQLFromTable(YSQLTable t, boolean only) {
            this.t = t;
            this.only = only;
        }

        public YSQLTable getTable() {
            return t;
        }

        public boolean isOnly() {
            return only;
        }

        @Override
        public YSQLDataType getExpressionType() {
            return null;
        }
    }

    public static class YSQLSubquery implements YSQLExpression {
        private final YSQLSelect s;
        private final String name;

        public YSQLSubquery(YSQLSelect s, String name) {
            this.s = s;
            this.name = name;
        }

        public YSQLSelect getSelect() {
            return s;
        }

        public String getName() {
            return name;
        }

        @Override
        public YSQLDataType getExpressionType() {
            return null;
        }
    }

}
