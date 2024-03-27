package sqlancer.postgres.ast;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.common.ast.newast.Aggregate;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresAggregate.PostgresAggregateFunction;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class PostgresAggregate extends FunctionNode<PostgresAggregateFunction, PostgresExpression>
        implements PostgresExpression, Aggregate<PostgresExpression, PostgresColumn> {

    public enum PostgresAggregateFunction {
        AVG(PostgresDataType.INT, PostgresDataType.FLOAT, PostgresDataType.REAL, PostgresDataType.DECIMAL),
        BIT_AND(PostgresDataType.INT), BIT_OR(PostgresDataType.INT), BOOL_AND(PostgresDataType.BOOLEAN),
        BOOL_OR(PostgresDataType.BOOLEAN), COUNT(PostgresDataType.INT), EVERY(PostgresDataType.BOOLEAN), MAX, MIN,
        // STRING_AGG
        SUM(PostgresDataType.INT, PostgresDataType.FLOAT, PostgresDataType.REAL, PostgresDataType.DECIMAL);

        private PostgresDataType[] supportedReturnTypes;

        PostgresAggregateFunction(PostgresDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public List<PostgresDataType> getTypes(PostgresDataType returnType) {
            return Arrays.asList(returnType);
        }

        public boolean supportsReturnType(PostgresDataType returnType) {
            return Arrays.asList(supportedReturnTypes).stream().anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public static List<PostgresAggregateFunction> getAggregates(PostgresDataType type) {
            return Arrays.asList(values()).stream().filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
        }

        public PostgresDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(PostgresDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

    public PostgresAggregate(List<PostgresExpression> args, PostgresAggregateFunction func) {
        super(func, args);
    }

    @Override
    public PostgresExpression asExpression() {
        return this;
    }

    @Override
    public String asString() {
        switch (getFunction()) {
        // case AVG:
        // return "SUM(agg0::DECIMAL)/SUM(agg1)::DECIMAL";
        case COUNT:
            return PostgresAggregateFunction.SUM.toString();
        default:
            return getFunction().toString();
        }
    }

    @Override
    public String asAggregatedString(String... from) {
        String combinedFrom = String.join(" UNION ALL ", from);
        return "SELECT " + asString() + " (agg0) FROM (" + combinedFrom + ") as asdf";
    }

}
