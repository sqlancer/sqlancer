package sqlancer.sqlite3.ast;

import sqlancer.Randomly;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public class SQLite3OrderingTerm implements SQLite3Expression {

    private final SQLite3Expression expression;
    private final Ordering ordering;

    public enum Ordering {
        ASC, DESC;

        public static Ordering getRandomValue() {
            return Randomly.fromOptions(Ordering.values());
        }
    }

    public SQLite3OrderingTerm(SQLite3Expression expression, Ordering ordering) {
        this.expression = expression;
        this.ordering = ordering;
    }

    public SQLite3Expression getExpression() {
        return expression;
    }

    public Ordering getOrdering() {
        return ordering;
    }

    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
        return expression.getExplicitCollateSequence();
    }

}
