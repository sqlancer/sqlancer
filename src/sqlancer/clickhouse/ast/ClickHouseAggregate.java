package sqlancer.clickhouse.ast;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseSchema;

public class ClickHouseAggregate extends ClickHouseExpression {

    private final ClickHouseAggregate.ClickHouseAggregateFunction func;
    private final List<ClickHouseExpression> expr;

    public enum ClickHouseAggregateFunction {
        AVG(ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32, ClickHouseDataType.Int64,
                ClickHouseDataType.UInt8, ClickHouseDataType.UInt16, ClickHouseDataType.UInt32,
                ClickHouseDataType.UInt64, ClickHouseDataType.Float32, ClickHouseDataType.Float64),
        BOOL_AND(ClickHouseDataType.UInt8), BOOL_OR(ClickHouseDataType.UInt8),
        COUNT(ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32, ClickHouseDataType.Int64,
                ClickHouseDataType.UInt8, ClickHouseDataType.UInt16, ClickHouseDataType.UInt32,
                ClickHouseDataType.UInt64, ClickHouseDataType.Float32, ClickHouseDataType.Float64,
                ClickHouseDataType.String),
        MAX, MIN,
        SUM(ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32, ClickHouseDataType.Int64,
                ClickHouseDataType.UInt8, ClickHouseDataType.UInt16, ClickHouseDataType.UInt32,
                ClickHouseDataType.UInt64, ClickHouseDataType.Float32, ClickHouseDataType.Float64);

        private ClickHouseDataType[] supportedReturnTypes;

        ClickHouseAggregateFunction(ClickHouseDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public static ClickHouseAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public static ClickHouseAggregateFunction getRandom(ClickHouseDataType type) {
            return Randomly.fromOptions(values());
        }

        public List<ClickHouseDataType> getTypes(ClickHouseDataType returnType) {
            return Arrays.asList(returnType);
        }

        public boolean supportsReturnType(ClickHouseDataType returnType) {
            return Arrays.asList(supportedReturnTypes).stream().anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public static List<ClickHouseAggregateFunction> getAggregates(ClickHouseDataType type) {
            return Arrays.asList(values()).stream().filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
        }

        public ClickHouseSchema.ClickHouseLancerDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return ClickHouseSchema.ClickHouseLancerDataType.getRandom();
            } else {
                return new ClickHouseSchema.ClickHouseLancerDataType(Randomly.fromOptions(supportedReturnTypes));
            }
        }

    }

    public ClickHouseAggregate(List<ClickHouseExpression> expr, ClickHouseAggregateFunction func) {
        this.expr = expr;
        this.func = func;
    }

    public ClickHouseAggregate.ClickHouseAggregateFunction getFunc() {
        return func;
    }

    public List<ClickHouseExpression> getExpr() {
        return expr;
    }

}
