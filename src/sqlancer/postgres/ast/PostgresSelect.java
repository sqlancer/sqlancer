package sqlancer.postgres.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;

public class PostgresSelect extends SelectBase<PostgresExpression>
        implements PostgresExpression, Select<PostgresJoin, PostgresExpression, PostgresTable, PostgresColumn> {

    private SelectType selectOption = SelectType.ALL;
    private List<PostgresJoin> joinClauses = Collections.emptyList();
    private PostgresExpression distinctOnClause;
    private ForClause forClause;

    public enum ForClause {
        UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE");

        private final String textRepresentation;

        ForClause(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static ForClause getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public static class PostgresFromTable implements PostgresExpression {
        private final PostgresTable t;
        private final boolean only;

        public PostgresFromTable(PostgresTable t, boolean only) {
            this.t = t;
            this.only = only;
        }

        public PostgresTable getTable() {
            return t;
        }

        public boolean isOnly() {
            return only;
        }

        @Override
        public PostgresDataType getExpressionType() {
            return null;
        }
    }

    public static class PostgresSubquery implements PostgresExpression {
        private final PostgresSelect s;
        private final String name;

        public PostgresSubquery(PostgresSelect s, String name) {
            this.s = s;
            this.name = name;
        }

        public PostgresSelect getSelect() {
            return s;
        }

        public String getName() {
            return name;
        }

        @Override
        public PostgresDataType getExpressionType() {
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

    public void setDistinctOnClause(PostgresExpression distinctOnClause) {
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
    public PostgresDataType getExpressionType() {
        return null;
    }

    @Override
    public void setJoinClauses(List<PostgresJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    @Override
    public List<PostgresJoin> getJoinClauses() {
        return joinClauses;
    }

    public PostgresExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setForClause(ForClause forClause) {
        this.forClause = forClause;
    }

    public ForClause getForClause() {
        return forClause;
    }

    @Override
    public String asString() {
        return PostgresVisitor.asString(this);
    }
}
