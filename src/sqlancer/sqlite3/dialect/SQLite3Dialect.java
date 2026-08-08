package sqlancer.sqlite3.dialect;

import java.util.List;

import sqlancer.simple.clause.Clause;
import sqlancer.simple.clause.Fetch;
import sqlancer.simple.clause.From;
import sqlancer.simple.clause.Join;
import sqlancer.simple.clause.Where;
import sqlancer.simple.dialect.Dialect;
import sqlancer.simple.expression.Between;
import sqlancer.simple.expression.BinaryArithmetic;
import sqlancer.simple.expression.BinaryComparison;
import sqlancer.simple.expression.BinaryLogical;
import sqlancer.simple.expression.Case;
import sqlancer.simple.expression.Cast;
import sqlancer.simple.expression.ColumnName;
import sqlancer.simple.expression.Constant;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.expression.In;
import sqlancer.simple.expression.IsNull;
import sqlancer.simple.expression.Like;
import sqlancer.simple.expression.Not;
import sqlancer.simple.expression.PrefixSign;
import sqlancer.simple.expression.SimilarTo;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.statement.SimpleSelect;
import sqlancer.simple.type.BigInt;
import sqlancer.simple.type.Bit;
import sqlancer.simple.type.Boolean;
import sqlancer.simple.type.Date;
import sqlancer.simple.type.Double;
import sqlancer.simple.type.Timestamp;
import sqlancer.simple.type.Type;
import sqlancer.simple.type.Varchar;
import sqlancer.sqlite3.dialect.expression.SQLite3Collate;
import sqlancer.sqlite3.dialect.expression.SQLite3Function;
import sqlancer.sqlite3.dialect.type.SQLite3Blob;

public class SQLite3Dialect implements Dialect {
    static List<Type> legalTypes = List.of(new BigInt(), new Bit(), new SQLite3Blob(), new Boolean(), new Date(),
            new Double(), new Timestamp(), new Varchar());
    static List<Class<? extends Expression>> legalExpressions = List.of(Between.class, BinaryArithmetic.class,
            BinaryComparison.class, BinaryLogical.class, Case.class, Cast.class, ColumnName.class, SQLite3Collate.class,
            Constant.class, SQLite3Function.class, In.class, IsNull.class, Like.class, Not.class, PrefixSign.class,
            SimilarTo.class);

    public static <T> boolean isOneOfClass(Class<? extends T> clazz, List<Class<? extends T>> clazzes) {
        return clazzes.contains(clazz);
    }

    @Override
    public Expression map(Expression expression) {
        if (isOneOfClass(expression.getClass(), legalExpressions)) {
            return expression;
        }
        // handle mapping of expressions
        throw new IllegalArgumentException("Expression not legal in dialect");
    }

    @Override
    public Clause map(Clause clause) {
        return clause;
    }

    @Override
    public List<Type> getTypes() {
        return legalTypes;
    }

    @Override
    public List<Class<? extends Expression>> getLegalExpressions() {
        return legalExpressions;
    }

    @Override
    public SimpleSelect generateSelect(Generator gen) {
        Clause fromClause = gen.generateClauseOfAny(List.of(From.class));
        Clause joinClause = gen.generateClauseOfAny(List.of(Join.Left.class, Join.Inner.class));
        Clause fetchClause = gen.generateClauseOfAny(List.of(Fetch.All.class, Fetch.Column.class));
        Clause whereClause = gen.generateClauseOfAny(List.of(Where.Of.class, Where.Empty.class));

        return new SimpleSelect(fetchClause, fromClause, joinClause, whereClause);
    }
}
