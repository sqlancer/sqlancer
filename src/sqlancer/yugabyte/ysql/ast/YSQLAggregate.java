package sqlancer.yugabyte.ysql.ast;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.ast.YSQLAggregate.YSQLAggregateFunction;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class YSQLAggregate extends FunctionNode<YSQLAggregateFunction, YSQLExpression> implements YSQLExpression {

    public YSQLAggregate(List<YSQLExpression> args, YSQLAggregateFunction func) {
        super(func, args);
    }

    public enum YSQLAggregateFunction {
        AVG(YSQLDataType.INT, YSQLDataType.FLOAT, YSQLDataType.REAL, YSQLDataType.DECIMAL), BIT_AND(YSQLDataType.INT),
        BIT_OR(YSQLDataType.INT), BOOL_AND(YSQLDataType.BOOLEAN), BOOL_OR(YSQLDataType.BOOLEAN),
        COUNT(YSQLDataType.INT), EVERY(YSQLDataType.BOOLEAN), MAX, MIN,
        // STRING_AGG
        SUM(YSQLDataType.INT, YSQLDataType.FLOAT, YSQLDataType.REAL, YSQLDataType.DECIMAL);

        private final YSQLDataType[] supportedReturnTypes;

        YSQLAggregateFunction(YSQLDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public static List<YSQLAggregateFunction> getAggregates(YSQLDataType type) {
            return Arrays.stream(values()).filter(p -> p.supportsReturnType(type)).collect(Collectors.toList());
        }

        public List<YSQLDataType> getTypes(YSQLDataType returnType) {
            return Collections.singletonList(returnType);
        }

        public boolean supportsReturnType(YSQLDataType returnType) {
            return Arrays.stream(supportedReturnTypes).anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public YSQLDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(YSQLDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

}
