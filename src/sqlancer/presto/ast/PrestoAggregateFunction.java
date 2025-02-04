package sqlancer.presto.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public enum PrestoAggregateFunction implements PrestoFunction {

    // General Aggregate Functions

    // arbitrary(x) → [same as input]
    // Returns an arbitrary non-null value of x, if one exists.
    ARBITRARY("arbitrary", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType() };
        }

        @Override
        public PrestoDataType getReturnType() {
            return PrestoDataType.getRandomWithoutNull();
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType.fromDataType(getReturnType()));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }

    },

    // TODO:
    //
    // array_agg(x) → array<[same as input]>#
    // Returns an array created from the input x elements.

    // avg(x) → double
    // Returns the average (arithmetic mean) of all input values.
    AVG("avg", PrestoDataType.FLOAT) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] {
                    Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT, PrestoDataType.DECIMAL) };
        }
    },
    // avg(time interval type) → time interval type#
    // Returns the average interval length of all input values.
    AVG_INTERVAL_YM("avg", PrestoDataType.INTERVAL_YEAR_TO_MONTH, PrestoDataType.INTERVAL_YEAR_TO_MONTH),
    AVG_INTERVAL_DS("avg", PrestoDataType.INTERVAL_DAY_TO_SECOND, PrestoDataType.INTERVAL_DAY_TO_SECOND),

    // bool_and(boolean) → boolean#
    // Returns TRUE if every input value is TRUE, otherwise FALSE.
    BOOL_AND("bool_and", PrestoDataType.BOOLEAN, PrestoDataType.BOOLEAN),
    // bool_or(boolean) → boolean#
    // Returns TRUE if any input value is TRUE, otherwise FALSE.
    BOOL_OR("bool_or", PrestoDataType.BOOLEAN, PrestoDataType.BOOLEAN),
    // checksum(x) → varbinary#
    // Returns an order-insensitive checksum of the given values.
    CHECKSUM("checksum", PrestoDataType.VARBINARY) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromList(PrestoDataType.getComparableTypes()) };
        }
    },

    // count(*) → bigint#
    // Returns the number of input rows.
    COUNT_ALL("count(*)", PrestoDataType.INT),
    // count(x) → bigint#
    // Returns the number of non-null input values.
    COUNT_NOARGS("count", PrestoDataType.INT), COUNT("count", PrestoDataType.INT) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromOptions(PrestoDataType.getRandomWithoutNull()) };
        }
    },
    // count_if(x) → bigint#
    // Returns the number of TRUE input values. This function is equivalent to count(CASE WHEN x THEN 1 END).
    COUNT_IF("count_if", PrestoDataType.INT) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromOptions(PrestoDataType.getRandomWithoutNull()) };
        }
    },
    // every(boolean) → boolean#
    // This is an alias for bool_and().
    EVERY("every", PrestoDataType.BOOLEAN, PrestoDataType.BOOLEAN),
    // geometric_mean(x) → double#
    // Returns the geometric mean of all input values.
    GEOMETRIC_MEAN("geometric_mean", PrestoDataType.FLOAT) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] {
                    Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT, PrestoDataType.DECIMAL) };
        }
    },
    // max_by(x, y) → [same as x]#
    // Returns the value of x associated with the maximum value of y over all input values.
    MAX_BY("max_by", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType(),
                    Randomly.fromList(PrestoDataType.getOrderableTypes()) };
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromList(PrestoDataType.getOrderableTypes());
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType.fromDataType(getReturnType()));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }

    },

    // TODO:
    //
    // max_by(x, y, n) → array<[same as x]>#
    // Returns n values of x associated with the n largest of all input values of y in descending order of y.

    // min_by(x, y) → [same as x]#
    // Returns the value of x associated with the minimum value of y over all input values.
    MIN_BY("min_by", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType(),
                    Randomly.fromList(PrestoDataType.getOrderableTypes()) };
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromList(PrestoDataType.getOrderableTypes());
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType.fromDataType(getReturnType()));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }

    },
    // TODO:
    //
    // min_by(x, y, n) → array<[same as x]>
    // Returns n values of x associated with the n smallest of all input values of y in ascending order of y.

    // max(x) → [same as input]
    // Returns the maximum value of all input values.
    MAX("max", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            boolean isCompatible = PrestoDataType.getOrderableTypes().contains(returnType.getPrimitiveDataType());
            if (returnType.getPrimitiveDataType() == PrestoDataType.ARRAY && returnType.toString().contains("JSON")) {
                isCompatible = false;
            }
            return isCompatible;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType() };
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromList(PrestoDataType.getOrderableTypes());
        }

        @Override
        public PrestoCompositeDataType getCompositeReturnType() {
            PrestoDataType dataType = Randomly.fromList(PrestoDataType.getOrderableTypes());
            PrestoCompositeDataType returnType;
            do {
                returnType = PrestoCompositeDataType.fromDataType(dataType);
            } while (!isCompatibleWithReturnType(returnType));
            return returnType;
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType.fromDataType(getReturnType()));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, true);
        }

    },

    // TODO:
    //
    // max(x, n) → array<[same as x]>#
    // Returns n largest values of all input values of x.

    // min(x) → [same as input]#
    // Returns the minimum value of all input values.
    MIN("min", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            boolean orderable = PrestoDataType.getOrderableTypes().contains(returnType.getPrimitiveDataType());
            if (returnType.getPrimitiveDataType() == PrestoDataType.ARRAY && returnType.toString().contains("JSON")) {
                orderable = false;
            }
            return orderable;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType() };
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromList(PrestoDataType.getOrderableTypes());
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType.fromDataType(getReturnType()));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }

    },

    // TODO:
    //
    // min(x, n) → array<[same as x]>#
    // Returns n smallest values of all input values of x.

    // TODO:
    //
    // reduce_agg(inputValue T, initialState S, inputFunction(S, T, S), combineFunction(S, S, S)) → S#
    // Reduces all input values into a single value. inputFunction will be invoked for each input value. In addition to
    // taking the input value, inputFunction takes the current state, initially initialState, and returns the new state.
    // combineFunction will be invoked to combine two states into a new state. The final state is returned:
    //
    // SELECT id, reduce_agg(value, (a, b) -> a + b, (a, b) -> a + b)
    // FROM (
    // VALUES
    // (1, 2),
    // (1, 3),
    // (1, 4),
    // (2, 20),
    // (2, 30),
    // (2, 40)
    // ) AS t(id, value)
    // GROUP BY id;
    // -- (1, 9)
    // -- (2, 90)
    //
    // SELECT id, reduce_agg(value, (a, b) -> a * b, (a, b) -> a * b)
    // FROM (
    // VALUES
    // (1, 2),
    // (1, 3),
    // (1, 4),
    // (2, 20),
    // (2, 30),
    // (2, 40)
    // ) AS t(id, value)
    // GROUP BY id;
    // -- (1, 24)
    // -- (2, 24000)
    // The state type must be a boolean, integer, floating-point, or date/time/interval.

    // TODO:
    //
    // set_agg(x) → array<[same as input]>#
    // Returns an array created from the distinct input x elements.

    // TODO:
    //
    // set_union(array(T)) -> array(T)#
    // Returns an array of all the distinct values contained in each array of the input
    //
    // Example:
    //
    // SELECT set_union(elements)
    // FROM (
    // VALUES
    // ARRAY[1, 3],
    // ARRAY[2, 4]
    // ) AS t(elements);
    // Returns ARRAY[1, 3, 4]

    // sum(x) → [same as input]#
    // Returns the sum of all input values.
    SUM("sum", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return List.of(PrestoDataType.INT, PrestoDataType.FLOAT, PrestoDataType.DECIMAL)
                    .contains(returnType.getPrimitiveDataType());
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType() };
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT, PrestoDataType.DECIMAL);
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType.fromDataType(getReturnType()));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }
    },
    // sum(time interval type) → time interval type#
    // Returns the average interval length of all input values.
    SUM_INTERVAL_YM("sum", PrestoDataType.INTERVAL_YEAR_TO_MONTH, PrestoDataType.INTERVAL_YEAR_TO_MONTH),
    SUM_INTERVAL_DS("sum", PrestoDataType.INTERVAL_DAY_TO_SECOND, PrestoDataType.INTERVAL_DAY_TO_SECOND),

    // Bitwise Aggregate Functions#

    // bitwise_and_agg(x) → bigint#
    // Returns the bitwise AND of all input values in 2’s complement representation.
    BITWISE_AND_AGG("bitwise_and_agg", PrestoDataType.INT, PrestoDataType.INT),

    // bitwise_or_agg(x) → bigint#
    // Returns the bitwise OR of all input values in 2’s complement representation.
    BITWISE_OR_AGG("bitwise_or_agg", PrestoDataType.INT, PrestoDataType.INT),

    // TODO:
    //
    // Map Aggregate Functions

    // histogram(x)#
    // Returns a map containing the count of the number of times each input value occurs.
    //
    // map_agg(key, value)#
    // Returns a map created from the input key / value pairs.
    //
    // map_union(x(K, V)) -> map(K, V)#
    // Returns the union of all the input maps. If a key is found in multiple input maps, that key’s value in the
    // resulting map comes from an arbitrary input map.
    //
    // map_union_sum(x(K, V)) -> map(K, V)#
    // Returns the union of all the input maps summing the values of matching keys in all the maps. All null values in
    // the original maps are coalesced to 0.
    //
    // multimap_agg(key, value)#
    // Returns a multimap created from the input key / value pairs. Each key can be associated with multiple values.

    // Approximate Aggregate Functions#
    // approx_distinct(x) → bigint#
    // Returns the approximate number of distinct input values. This function provides an approximation of
    // count(DISTINCT x).
    // Zero is returned if all input values are null.
    // This function should produce a standard error of 2.3%, which is the standard deviation of the (approximately
    // normal)
    // error distribution over all possible sets. It does not guarantee an upper bound on the error for any specific
    // input set.
    APPROX_DISTINCT("approx_distinct", PrestoDataType.INT) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromList(PrestoDataType.getOrderableTypes()) };
        }
    },
    //
    // approx_distinct(x, e) → bigint#
    // Returns the approximate number of distinct input values. This function provides an approximation of
    // count(DISTINCT x). Zero is returned if all input values are null.
    //
    // This function should produce a standard error of no more than e, which is the standard deviation of the
    // (approximately normal) error distribution over all possible sets. It does not guarantee an upper bound on the
    // error for any specific input set. The current implementation of this function requires that e be in the range of
    // [0.0040625, 0.26000].
    APPROX_DISTINCT_2("approx_distinct", PrestoDataType.INT) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromList(PrestoDataType.getOrderableTypes()), PrestoDataType.FLOAT };
        }
    },
    // approx_percentile(x, percentage) → [same as x]#
    // Returns the approximate percentile for all input values of x at the given percentage.
    // The value of percentage must be between zero and one and must be constant for all input rows.
    APPROX_PERCENTILE("approx_percentile", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return List.of(PrestoDataType.INT, PrestoDataType.FLOAT).contains(returnType.getPrimitiveDataType());
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT),
                    PrestoDataType.FLOAT };
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoDataType[] argumentTypes2, PrestoCompositeDataType returnType2) {
            List<Node<PrestoExpression>> arguments = new ArrayList<>();
            arguments.add(gen.generateExpression(returnType2, depth + 1));
            arguments.add(new PrestoConstant.PrestoFloatConstant(Randomly.getPercentage()));
            return arguments;
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT);
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType.fromDataType(getReturnType()));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }
    },

    // approx_percentile(x, percentage, accuracy) → [same as x]#
    // As approx_percentile(x, percentage), but with a maximum rank error of accuracy.
    // The value of accuracy must be between zero and one (exclusive) and must be constant for all input rows.
    // Note that a lower “accuracy” is really a lower error threshold, and thus more accurate. The default accuracy is
    // 0.01.
    APPROX_PERCENTILE_ACCURACY("approx_percentile", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return List.of(PrestoDataType.INT, PrestoDataType.FLOAT).contains(returnType.getPrimitiveDataType());
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT),
                    PrestoDataType.FLOAT, PrestoDataType.FLOAT };
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoDataType[] argumentTypes2, PrestoCompositeDataType returnType2) {
            List<Node<PrestoExpression>> arguments = new ArrayList<>();
            arguments.add(gen.generateExpression(returnType2, depth + 1));
            arguments.add(new PrestoConstant.PrestoFloatConstant(Randomly.getPercentage()));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                arguments.add(new PrestoConstant.PrestoFloatConstant(0.01D));
            } else {
                arguments.add(new PrestoConstant.PrestoFloatConstant(Randomly.getPercentage()));
            }
            return arguments;
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT);
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType
                            .fromDataType(Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT)));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }
    },

    // TODO:
    //
    // approx_percentile(x, percentages) → array<[same as x]>#
    // Returns the approximate percentile for all input values of x at each of the specified percentages. Each element
    // of the percentages array must be between zero and one, and the array must be constant for all input rows.
    //
    // approx_percentile(x, percentages, accuracy) → array<[same as x]>#
    // As approx_percentile(x, percentages), but with a maximum rank error of accuracy.

    // approx_percentile(x, w, percentage) → [same as x]#
    // Returns the approximate weighed percentile for all input values of x using the per-item weight w at the
    // percentage p.
    // The weight must be an integer value of at least one.
    // It is effectively a replication count for the value x in the percentile set.
    // The value of p must be between zero and one and must be constant for all input rows.
    APPROX_PERCENTILE_WEIGHT("approx_percentile", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return List.of(PrestoDataType.INT, PrestoDataType.FLOAT).contains(returnType.getPrimitiveDataType());
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT),
                    PrestoDataType.INT, PrestoDataType.FLOAT };
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoDataType[] argumentTypes2, PrestoCompositeDataType returnType2) {
            List<Node<PrestoExpression>> arguments = new ArrayList<>();
            arguments.add(gen.generateExpression(returnType2, depth + 1));
            arguments.add(new PrestoConstant.PrestoFloatConstant(Randomly.getPercentage()));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                arguments.add(new PrestoConstant.PrestoIntConstant(1));
            } else {
                arguments.add(new PrestoConstant.PrestoIntConstant(Randomly.smallNumber()));
            }
            return arguments;
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT);
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType
                            .fromDataType(Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT)));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }
    },

    // approx_percentile(x, w, percentage, accuracy) → [same as x]#
    // As approx_percentile(x, w, percentage), but with a maximum rank error of accuracy.
    APPROX_PERCENTILE_PERCENTAGE_ACCURACY("approx_percentile", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return List.of(PrestoDataType.INT, PrestoDataType.FLOAT).contains(returnType.getPrimitiveDataType());
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT),
                    PrestoDataType.INT, PrestoDataType.FLOAT, PrestoDataType.FLOAT };
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoDataType[] argumentTypes2, PrestoCompositeDataType returnType2) {
            List<Node<PrestoExpression>> arguments = new ArrayList<>();
            arguments.add(gen.generateExpression(returnType2, depth + 1));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                arguments.add(new PrestoConstant.PrestoIntConstant(1));
            } else {
                arguments.add(new PrestoConstant.PrestoIntConstant(Randomly.smallNumber()));
            }
            arguments.add(new PrestoConstant.PrestoFloatConstant(Randomly.getPercentage()));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                arguments.add(new PrestoConstant.PrestoFloatConstant(0.01D));
            } else {
                arguments.add(new PrestoConstant.PrestoFloatConstant(Randomly.getPercentage()));
            }
            return arguments;
        }

        @Override
        public PrestoDataType getReturnType() {
            return Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT);
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoCompositeDataType returnType, boolean orderable) {
            PrestoCompositeDataType returnTypeLocal = Objects.requireNonNullElseGet(returnType,
                    () -> PrestoCompositeDataType
                            .fromDataType(Randomly.fromOptions(PrestoDataType.INT, PrestoDataType.FLOAT)));
            return super.getArgumentsForReturnType(gen, depth, returnTypeLocal, orderable);
        }
    };

    // TODO:
    //
    // approx_percentile(x, w, percentages) → array<[same as x]>#
    // Returns the approximate weighed percentile for all input values of x using the per-item weight w at each of the
    // given percentages specified in the array. The weight must be an integer value of at least one. It is effectively
    // a replication count for the value x in the percentile set. Each element of the array must be between zero and
    // one, and the array must be constant for all input rows.
    //
    // approx_percentile(x, w, percentages, accuracy) → array<[same as x]>#
    // As approx_percentile(x, w, percentages), but with a maximum rank error of accuracy.
    //
    // approx_set(x) → HyperLogLog
    // See HyperLogLog Functions.
    //
    // merge(x) → HyperLogLog
    // See HyperLogLog Functions.
    //
    // khyperloglog_agg(x) → KHyperLogLog
    // See KHyperLogLog Functions.

    // TODO:
    //
    // merge(qdigest(T)) -> qdigest(T)
    // See Quantile Digest Functions.
    //
    // qdigest_agg(x) → qdigest<[same as x]>
    // See Quantile Digest Functions.
    //
    // qdigest_agg(x, w) → qdigest<[same as x]>
    // See Quantile Digest Functions.
    //
    // qdigest_agg(x, w, accuracy) → qdigest<[same as x]>
    // See Quantile Digest Functions.
    //
    // numeric_histogram(buckets, value, weight) → map<double, double>#
    // Computes an approximate histogram with up to buckets number of buckets for all values with a per-item weight of
    // weight.
    // The keys of the returned map are roughly the center of the bin, and the entry is the total weight of the bin.
    // The algorithm is based loosely on [BenHaimTomTov2010].
    //
    // buckets must be a bigint. value and weight must be numeric.
    //
    // numeric_histogram(buckets, value) → map<double, double>#
    // Computes an approximate histogram with up to buckets number of buckets for all values. This function is
    // equivalent to the variant of numeric_histogram() that takes a weight, with a per-item weight of 1. In this case,
    // the total weight in the returned map is the count of items in the bin.

    private final PrestoDataType returnType;
    private final PrestoDataType[] argumentTypes;
    private final String functionName;

    PrestoAggregateFunction(String functionName, PrestoDataType returnType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argumentTypes = new PrestoDataType[0];
    }

    PrestoAggregateFunction(String functionName, PrestoDataType returnType, PrestoDataType... argumentTypes) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes.clone();
    }

    public static PrestoAggregateFunction getRandomMetamorphicOracle() {
        return Randomly.fromOptions(ARBITRARY, AVG, AVG_INTERVAL_YM, AVG_INTERVAL_DS, BOOL_AND, BOOL_OR, CHECKSUM,
                COUNT_ALL, COUNT_NOARGS, COUNT, COUNT_IF, EVERY, GEOMETRIC_MEAN, MAX_BY, MIN_BY, MAX, MIN, SUM,
                SUM_INTERVAL_YM, SUM_INTERVAL_DS, BITWISE_AND_AGG, BITWISE_OR_AGG);
    }

    public static PrestoAggregateFunction getRandom() {
        return Randomly.fromOptions(values());
    }

    public static List<PrestoAggregateFunction> getFunctionsCompatibleWith(PrestoCompositeDataType returnType) {
        return Stream.of(values()).filter(f -> f.isCompatibleWithReturnType(returnType)).collect(Collectors.toList());
    }

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
        return this.returnType == returnType.getPrimitiveDataType();
    }

    @Override
    public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
        return argumentTypes.clone();
    }

    @Override
    public int getNumberOfArguments() {
        return 1;
    }

    public List<PrestoSchema.PrestoDataType> getReturnTypes(PrestoSchema.PrestoDataType dataType) {
        return Collections.singletonList(dataType);
    }

    public PrestoDataType getReturnType() {
        if (returnType == null) {
            return PrestoDataType.getRandomWithoutNull();
        }
        return returnType;
    }

    public PrestoCompositeDataType getCompositeReturnType() {
        PrestoDataType dataType = getReturnType();
        return PrestoCompositeDataType.fromDataType(dataType);
    }
}
