package sqlancer.cockroachdb.ast;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;

public class CockroachDBAggregate implements CockroachDBExpression {

    private CockroachDBAggregateFunction func;
    private List<CockroachDBExpression> expr;

    public enum CockroachDBAggregateFunction {
        SUM(CockroachDBDataType.INT, CockroachDBDataType.FLOAT, CockroachDBDataType.DECIMAL), //
        SUM_INT(CockroachDBDataType.INT), //
        AVG(CockroachDBDataType.INT, CockroachDBDataType.FLOAT, CockroachDBDataType.DECIMAL), //
        MIN() {
            @Override
            public boolean supportsReturnType(CockroachDBDataType returnType) {
                return true;
            }
        }, //
        MAX() {
            @Override
            public boolean supportsReturnType(CockroachDBDataType returnType) {
                return true;
            }
        }, //
        COUNT_ROWS(CockroachDBDataType.INT) {
            @Override
            public List<CockroachDBDataType> getTypes(CockroachDBDataType returnType) {
                return Collections.emptyList();
            }
        }, //
        COUNT(CockroachDBDataType.INT) {

            @Override
            public List<CockroachDBDataType> getTypes(CockroachDBDataType returnType) {
                return Arrays.asList(CockroachDBDataType.getRandom());
            }
        }, //
        SQRDIFF(CockroachDBDataType.INT, CockroachDBDataType.FLOAT, CockroachDBDataType.DECIMAL), //
        STDDEV(CockroachDBDataType.INT, CockroachDBDataType.FLOAT, CockroachDBDataType.DECIMAL), //
        VARIANCE(CockroachDBDataType.INT, CockroachDBDataType.FLOAT, CockroachDBDataType.DECIMAL), //
        XOR_AGG(CockroachDBDataType.BYTES, CockroachDBDataType.INT), //
        BIT_AND(CockroachDBDataType.INT), //
        BIT_OR(CockroachDBDataType.INT), //
        BOOL_AND(CockroachDBDataType.BOOL), //
        BOOL_OR(CockroachDBDataType.BOOL), STRING_AGG(CockroachDBDataType.STRING, CockroachDBDataType.BYTES) {
            @Override
            public List<CockroachDBDataType> getTypes(CockroachDBDataType returnType) {
                return Arrays.asList(returnType, returnType);
            }
        }, //
        CONCAT_AGG(CockroachDBDataType.STRING, CockroachDBDataType.BYTES);

        private CockroachDBDataType[] supportedReturnTypes;

        public List<CockroachDBDataType> getTypes(CockroachDBDataType returnType) {
            return Arrays.asList(returnType);
        }

        public boolean supportsReturnType(CockroachDBDataType returnType) {
            return Arrays.asList(supportedReturnTypes).stream().anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public static List<CockroachDBAggregateFunction> getAggregates(CockroachDBDataType type) {
            return Arrays.asList(values()).stream().filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
        }

        //
        CockroachDBAggregateFunction(CockroachDBDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public static CockroachDBAggregateFunction getRandomMetamorphicOracle() {
            // not: VARIANCE, STDDEV, SQRDIFF
            return Randomly.fromOptions(SUM, SUM_INT, MIN, MAX, XOR_AGG, BIT_AND, BIT_OR, BOOL_AND, BOOL_OR, COUNT, AVG,
                    COUNT_ROWS);
        }

        public CockroachDBDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(CockroachDBDataType.getRandom());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }
    }

    public CockroachDBAggregate(CockroachDBAggregateFunction func, List<CockroachDBExpression> expr) {
        this.func = func;
        this.expr = expr;
    }

    public CockroachDBAggregateFunction getFunc() {
        return func;
    }

    public List<CockroachDBExpression> getExpr() {
        return expr;
    }

}
