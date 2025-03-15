package sqlancer.postgres.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowFrame;

public class PostgresSelect extends SelectBase<PostgresExpression>
        implements PostgresExpression, Select<PostgresJoin, PostgresExpression, PostgresTable, PostgresColumn> {

    private SelectType selectOption = SelectType.ALL;
    private List<PostgresJoin> joinClauses = Collections.emptyList();
    private PostgresExpression distinctOnClause;
    private ForClause forClause;
    private List<PostgresExpression> windowFunctions = new ArrayList<>();
    private final Map<String, WindowDefinition> windowDefinitions = new HashMap<>();

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

    public static class WindowDefinition {
        private final List<PostgresExpression> partitionBy;
        private final List<PostgresOrderByTerm> orderBy;
        private final WindowFrame frame;

        public WindowDefinition(List<PostgresExpression> partitionBy, List<PostgresOrderByTerm> orderBy,
                WindowFrame frame) {
            this.partitionBy = partitionBy;
            this.orderBy = orderBy;
            this.frame = frame;
        }

        public List<PostgresExpression> getPartitionBy() {
            return partitionBy;
        }

        public List<PostgresOrderByTerm> getOrderBy() {
            return orderBy;
        }

        public WindowFrame getFrame() {
            return frame;
        }
    }

    // Getters setters for windowfunctions
    public List<PostgresExpression> getWindowFunctions() {
        return windowFunctions;
    }

    public void setWindowFunctions(List<PostgresExpression> windowFunctions) {
        this.windowFunctions = windowFunctions;
    }

    // Add methods for window definitions
    public void addWindowDefinition(String name, WindowDefinition definition) {
        windowDefinitions.put(name, definition);
    }

    public WindowDefinition getWindowDefinition(String name) {
        return windowDefinitions.get(name);
    }

    public Map<String, WindowDefinition> getWindowDefinitions() {
        return windowDefinitions;
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
