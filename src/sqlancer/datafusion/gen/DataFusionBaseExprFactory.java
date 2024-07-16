package sqlancer.datafusion.gen;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;
import static sqlancer.datafusion.gen.DataFusionBaseExpr.createCommonNumericAggrFuncSingleArg;
import static sqlancer.datafusion.gen.DataFusionBaseExpr.createCommonNumericFuncSingleArg;
import static sqlancer.datafusion.gen.DataFusionBaseExpr.createCommonNumericFuncTwoArgs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.datafusion.DataFusionSchema.DataFusionDataType;
import sqlancer.datafusion.gen.DataFusionBaseExpr.ArgumentType;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprCategory;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprType;

public final class DataFusionBaseExprFactory {
    private DataFusionBaseExprFactory() {
        dfAssert(false, "Utility class cannot be instantiated");
    }

    public static DataFusionBaseExpr createExpr(DataFusionBaseExprType type) {
        switch (type) {
        case IS_NULL:
            return new DataFusionBaseExpr("IS NULL", 1, DataFusionBaseExprCategory.UNARY_POSTFIX,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                            DataFusionDataType.DOUBLE, DataFusionDataType.BIGINT, DataFusionDataType.NULL)))));
        case IS_NOT_NULL:
            return new DataFusionBaseExpr("IS NOT NULL", 1, DataFusionBaseExprCategory.UNARY_POSTFIX,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                            DataFusionDataType.DOUBLE, DataFusionDataType.BIGINT, DataFusionDataType.NULL)))));
        case BITWISE_AND:
            return new DataFusionBaseExpr("&", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case BITWISE_OR:
            return new DataFusionBaseExpr("|", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case BITWISE_XOR:
            return new DataFusionBaseExpr("^", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case BITWISE_SHIFT_RIGHT:
            return new DataFusionBaseExpr(">>", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case BITWISE_SHIFT_LEFT:
            return new DataFusionBaseExpr("<<", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case NOT:
            return new DataFusionBaseExpr("NOT", 1, DataFusionBaseExprCategory.UNARY_PREFIX,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN)))));
        case PLUS: // unary prefix '+'
            return new DataFusionBaseExpr("+", 1, DataFusionBaseExprCategory.UNARY_PREFIX,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(new ArgumentType.Fixed(
                            new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case MINUS: // unary prefix '-'
            return new DataFusionBaseExpr("-", 1, DataFusionBaseExprCategory.UNARY_PREFIX,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(new ArgumentType.Fixed(
                            new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case MULTIPLICATION:
            return new DataFusionBaseExpr("*", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case DIVISION:
            return new DataFusionBaseExpr("/", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case MODULO:
            return new DataFusionBaseExpr("%", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case EQUAL:
            return new DataFusionBaseExpr("=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case EQUAL2:
            return new DataFusionBaseExpr("==", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case NOT_EQUAL:
            return new DataFusionBaseExpr("!=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case LESS_THAN:
            return new DataFusionBaseExpr("<", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case LESS_THAN_OR_EQUAL_TO:
            return new DataFusionBaseExpr("<=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case GREATER_THAN:
            return new DataFusionBaseExpr(">", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case GREATER_THAN_OR_EQUAL_TO:
            return new DataFusionBaseExpr(">=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case IS_DISTINCT_FROM:
            return new DataFusionBaseExpr("IS DISTINCT FROM", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case IS_NOT_DISTINCT_FROM:
            return new DataFusionBaseExpr("IS NOT DISTINCT FROM", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case AND:
            return new DataFusionBaseExpr("AND", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))) // arg2
                    ));
        case OR:
            return new DataFusionBaseExpr("OR", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))) // arg2
                    ));
        case ADD: // binary arithmetic operator '+'
            return new DataFusionBaseExpr("+", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))) // arg2
                    ));
        case SUB: // binary arithmetic operator '-'
            return new DataFusionBaseExpr("-", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))) // arg2
                    ));
        case FUNC_ABS:
            return createCommonNumericFuncSingleArg("ABS");
        case FUNC_ACOS:
            return createCommonNumericFuncSingleArg("ACOS");
        case FUNC_ACOSH:
            return createCommonNumericFuncSingleArg("ACOSH");
        case FUNC_ASIN:
            return createCommonNumericFuncSingleArg("ASIN");
        case FUNC_ASINH:
            return createCommonNumericFuncSingleArg("ASINH");
        case FUNC_ATAN:
            return createCommonNumericFuncSingleArg("ATAN");
        case FUNC_ATANH:
            return createCommonNumericFuncSingleArg("ATANH");
        case FUNC_ATAN2:
            return createCommonNumericFuncTwoArgs("ATAN2");
        case FUNC_CBRT:
            return createCommonNumericFuncSingleArg("CBRT");
        case FUNC_CEIL:
            return createCommonNumericFuncSingleArg("CEIL");
        case FUNC_COS:
            return createCommonNumericFuncSingleArg("COS");
        case FUNC_COSH:
            return createCommonNumericFuncSingleArg("COSH");
        case FUNC_DEGREES:
            return createCommonNumericFuncSingleArg("DEGREES");
        case FUNC_EXP:
            return createCommonNumericFuncSingleArg("EXP");
        case FUNC_FACTORIAL:
            return createCommonNumericFuncSingleArg("FACTORIAL");
        case FUNC_FLOOR:
            return createCommonNumericFuncSingleArg("FLOOR");
        case FUNC_GCD:
            return new DataFusionBaseExpr("GCD", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_ISNAN:
            return createCommonNumericFuncSingleArg("ISNAN");
        case FUNC_ISZERO:
            return createCommonNumericFuncSingleArg("ISZERO");
        case FUNC_LCM:
            return createCommonNumericFuncTwoArgs("LCM");
        case FUNC_LN:
            return createCommonNumericFuncSingleArg("LN");
        case FUNC_LOG:
            return createCommonNumericFuncSingleArg("LOG");
        case FUNC_LOG_WITH_BASE:
            return createCommonNumericFuncTwoArgs("LOG");
        case FUNC_LOG10:
            return createCommonNumericFuncSingleArg("LOG10");
        case FUNC_LOG2:
            return createCommonNumericFuncSingleArg("LOG2");
        case FUNC_NANVL:
            return createCommonNumericFuncTwoArgs("NANVL");
        case FUNC_PI:
            return new DataFusionBaseExpr("PI", 0, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE), Arrays.asList());
        case FUNC_POW:
            return createCommonNumericFuncSingleArg("POW");
        case FUNC_POWER:
            return createCommonNumericFuncSingleArg("POWER");
        case FUNC_RADIANS:
            return createCommonNumericFuncSingleArg("RADIANS");
        case FUNC_ROUND:
            return createCommonNumericFuncSingleArg("ROUND");
        case FUNC_ROUND_WITH_DECIMAL:
            return new DataFusionBaseExpr("ROUND", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_SIGNUM:
            return createCommonNumericFuncSingleArg("SIGNUM");
        case FUNC_SIN:
            return createCommonNumericFuncSingleArg("SIN");
        case FUNC_SINH:
            return createCommonNumericFuncSingleArg("SINH");
        case FUNC_SQRT:
            return createCommonNumericFuncSingleArg("SQRT");
        case FUNC_TAN:
            return createCommonNumericFuncSingleArg("TAN");
        case FUNC_TANH:
            return createCommonNumericFuncSingleArg("TANH");
        case FUNC_TRUNC:
            return createCommonNumericFuncSingleArg("TRUNC");
        case FUNC_TRUNC_WITH_DECIMAL:
            return new DataFusionBaseExpr("TRUNC", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_COALESCE:
            return new DataFusionBaseExpr("COALESCE", -1, // overide by variadic
                    DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE), Arrays.asList(), true);
        case FUNC_NULLIF:
            return new DataFusionBaseExpr("NULLIF", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_NVL:
            return new DataFusionBaseExpr("NVL", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_NVL2:
            return new DataFusionBaseExpr("NVL2", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_IFNULL:
            return new DataFusionBaseExpr("IFNULL", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));

        case AGGR_MIN:
            return createCommonNumericAggrFuncSingleArg("MIN");
        case AGGR_MAX:
            return createCommonNumericAggrFuncSingleArg("MAX");
        case AGGR_AVG:
            return createCommonNumericAggrFuncSingleArg("AVG");
        case AGGR_SUM:
            return createCommonNumericAggrFuncSingleArg("SUM");
        case AGGR_COUNT:
            return new DataFusionBaseExpr("COUNT", -1, DataFusionBaseExprCategory.AGGREGATE,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                            DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))),
                    true);
        default:
            dfAssert(false, "Unreachable. Unimplemented branch for type " + type);
        }

        dfAssert(false, "Unreachable. Unimplemented branch for type " + type);
        return null;
    }

    // if input is Optional.empty(), return all possible `DataFusionBaseExpr`s
    // else, return all `DataFusionBaseExpr` which might be evaluated to arg's type
    public static List<DataFusionBaseExpr> getExprsWithReturnType(Optional<DataFusionDataType> dataTypeOptional) {
        List<DataFusionBaseExpr> allExpressions = Arrays.stream(DataFusionBaseExprType.values())
                .map(DataFusionBaseExprFactory::createExpr).collect(Collectors.toList());

        if (!dataTypeOptional.isPresent()) {
            return allExpressions; // If Optional is empty, return all expressions
        }

        DataFusionDataType filterType = dataTypeOptional.get();
        List<DataFusionBaseExpr> exprsWithReturnType = allExpressions.stream()
                .filter(expr -> expr.possibleReturnTypes.contains(filterType)).collect(Collectors.toList());

        if (Randomly.getBoolean()) {
            // Too many similar function, so test them less often
            return exprsWithReturnType;
        }

        return exprsWithReturnType.stream().filter(expr -> expr.exprType != DataFusionBaseExprCategory.FUNC)
                .collect(Collectors.toList());
    }

    public static DataFusionBaseExpr getRandomAggregateExpr() {
        List<DataFusionBaseExpr> allAggrExpressions = Arrays.stream(DataFusionBaseExprType.values())
                .map(DataFusionBaseExprFactory::createExpr)
                .filter(expr -> expr.exprType == DataFusionBaseExprCategory.AGGREGATE).collect(Collectors.toList());

        return Randomly.fromList(allAggrExpressions);
    }
}
