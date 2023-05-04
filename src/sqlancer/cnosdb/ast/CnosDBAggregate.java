package sqlancer.cnosdb.ast;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBBugs;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBAggregate.CnosDBAggregateFunction;
import sqlancer.common.ast.FunctionNode;

public class CnosDBAggregate extends FunctionNode<CnosDBAggregateFunction, CnosDBExpression>
        implements CnosDBExpression {

    public CnosDBAggregate(List<CnosDBExpression> args, CnosDBAggregateFunction func) {
        super(func, args);
    }

    public enum CnosDBAggregateFunction {
        AVG(CnosDBDataType.DOUBLE),
        MAX(CnosDBDataType.DOUBLE, CnosDBDataType.INT, CnosDBDataType.STRING, CnosDBDataType.TIMESTAMP,
                CnosDBDataType.UINT),
        MIN(CnosDBDataType.DOUBLE, CnosDBDataType.INT, CnosDBDataType.STRING, CnosDBDataType.TIMESTAMP,
                CnosDBDataType.UINT),
        COUNT(CnosDBDataType.INT) {
            @Override
            public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[] { CnosDBDataType.getRandomType() };
            }
        },
        SUM(CnosDBDataType.INT, CnosDBDataType.DOUBLE, CnosDBDataType.UINT), APPROX_MEDIAN(CnosDBDataType.DOUBLE),

        VAR(CnosDBDataType.DOUBLE), VAR_SAMP(CnosDBDataType.DOUBLE), VAR_POP(CnosDBDataType.DOUBLE),
        STDDEV(CnosDBDataType.DOUBLE), STDDEV_SAMP(CnosDBDataType.DOUBLE), STDDEV_POP(CnosDBDataType.DOUBLE),
        COVAR(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[] { CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE };
            }
        },
        COVAR_SAMP(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[] { CnosDBDataType.DOUBLE, CnosDBDataType.INT };
            }
        },
        CORR(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[] { CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE };
            }
        },
        COVAR_POP(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[] { CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE };
            }
        },

        APPROX_PERCENTILE_CONT(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[] { CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE };
            }
        },
        APPROX_PERCENTILE_CONT_WITH_WEIGHT(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[] { CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE };
            }
        },
        APPROX_DISTINCT(CnosDBDataType.UINT), GROUPING(CnosDBDataType.INT), ARRAY_AGG(CnosDBDataType.STRING);

        private final CnosDBDataType[] supportedReturnTypes;

        CnosDBAggregateFunction(CnosDBDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public static List<CnosDBAggregateFunction> getAggregates(CnosDBDataType type) {
            List<CnosDBAggregateFunction> res = Stream.of(values()).filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
            if (CnosDBBugs.BUG786) {
                res.removeAll(List.of(VAR, VAR_POP, VAR_SAMP, STDDEV, STDDEV_POP, STDDEV_SAMP, CORR, COVAR, COVAR_POP,
                        COVAR_SAMP, APPROX_PERCENTILE_CONT_WITH_WEIGHT, APPROX_DISTINCT, APPROX_PERCENTILE_CONT,
                        APPROX_PERCENTILE_CONT_WITH_WEIGHT, GROUPING, ARRAY_AGG));
            }

            return res;
        }

        public CnosDBDataType[] getArgsTypes(CnosDBDataType returnType) {
            return new CnosDBDataType[] { returnType };
        }

        public boolean supportsReturnType(CnosDBDataType returnType) {
            return Arrays.stream(supportedReturnTypes).anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public CnosDBDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(CnosDBDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

}
