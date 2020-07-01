package sqlancer.sqlite3.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public class SQLite3WindowFunctionExpression extends SQLite3Expression {

    private final SQLite3Expression baseWindowFunction; // also contains the arguments to the window function
    private List<SQLite3Expression> partitionBy = new ArrayList<>();
    private List<SQLite3Expression> orderBy = new ArrayList<>();
    private SQLite3Expression filterClause;
    private SQLite3Expression frameSpec;
    private SQLite3FrameSpecExclude exclude;
    private SQLite3FrameSpecKind frameSpecKind;

    public static class SQLite3WindowFunctionFrameSpecTerm extends SQLite3Expression {

        public enum SQLite3WindowFunctionFrameSpecTermKind {
            UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"), EXPR_PRECEDING("PRECEDING"), CURRENT_ROW("CURRENT ROW"),
            EXPR_FOLLOWING("FOLLOWING"), UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING");

            String s;

            SQLite3WindowFunctionFrameSpecTermKind(String s) {
                this.s = s;
            }

            public String getString() {
                return s;
            }

        }

        private final SQLite3Expression expression;
        private final SQLite3WindowFunctionFrameSpecTermKind kind;

        public SQLite3WindowFunctionFrameSpecTerm(SQLite3Expression expression,
                SQLite3WindowFunctionFrameSpecTermKind kind) {
            this.expression = expression;
            this.kind = kind;
        }

        public SQLite3WindowFunctionFrameSpecTerm(SQLite3WindowFunctionFrameSpecTermKind kind) {
            this.kind = kind;
            this.expression = null;
        }

        public SQLite3Expression getExpression() {
            return expression;
        }

        public SQLite3WindowFunctionFrameSpecTermKind getKind() {
            return kind;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class SQLite3WindowFunctionFrameSpecBetween extends SQLite3Expression {

        private final SQLite3WindowFunctionFrameSpecTerm left;
        private final SQLite3WindowFunctionFrameSpecTerm right;

        public SQLite3WindowFunctionFrameSpecBetween(SQLite3WindowFunctionFrameSpecTerm left,
                SQLite3WindowFunctionFrameSpecTerm right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return null;
        }

        public SQLite3WindowFunctionFrameSpecTerm getLeft() {
            return left;
        }

        public SQLite3WindowFunctionFrameSpecTerm getRight() {
            return right;
        }

    }

    public enum SQLite3FrameSpecExclude {
        EXCLUDE_NO_OTHERS("EXCLUDE NO OTHERS"), EXCLUDE_CURRENT_ROW("EXCLUDE CURRENT ROW"),
        EXCLUDE_GROUP("EXCLUDE GROUP"), EXCLUDE_TIES("EXCLUDE TIES");

        private final String s;

        SQLite3FrameSpecExclude(String s) {
            this.s = s;
        }

        public static SQLite3FrameSpecExclude getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getString() {
            return s;
        }
    }

    public enum SQLite3FrameSpecKind {
        RANGE, ROWS, GROUPS;

        public static SQLite3FrameSpecKind getRandom() {
            return Randomly.fromOptions(SQLite3FrameSpecKind.values());
        }
    }

    public SQLite3WindowFunctionExpression(SQLite3Expression baseWindowFunction) {
        this.baseWindowFunction = baseWindowFunction;
    }

    public SQLite3Expression getBaseWindowFunction() {
        return baseWindowFunction;
    }

    public List<SQLite3Expression> getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(List<SQLite3Expression> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public List<SQLite3Expression> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<SQLite3Expression> orderBy) {
        this.orderBy = orderBy;
    }

    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    public SQLite3Expression getFilterClause() {
        return filterClause;
    }

    public void setFilterClause(SQLite3Expression filterClause) {
        this.filterClause = filterClause;
    }

    public SQLite3Expression getFrameSpec() {
        return frameSpec;
    }

    public void setFrameSpec(SQLite3Expression frameSpec) {
        this.frameSpec = frameSpec;
    }

    public SQLite3FrameSpecExclude getExclude() {
        return exclude;
    }

    public void setExclude(SQLite3FrameSpecExclude exclude) {
        this.exclude = exclude;
    }

    public SQLite3FrameSpecKind getFrameSpecKind() {
        return frameSpecKind;
    }

    public void setFrameSpecKind(SQLite3FrameSpecKind frameSpecKind) {
        this.frameSpecKind = frameSpecKind;
    }

}
