package sqlancer.oxla.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.ArrayList;
import java.util.List;

public class OxlaFunctionOperation extends NewFunctionNode<OxlaExpression, OxlaFunctionOperation.OxlaFunction>
        implements OxlaExpression {
    public OxlaFunctionOperation(List<OxlaExpression> args, OxlaFunction func) {
        super(args, func);
    }

    @Override
    public OxlaConstant getExpectedValue() {
        OxlaConstant[] expectedValues = new OxlaConstant[args.size()];
        for (int index = 0; index < args.size(); ++index) {
            if (args.get(index) == null) {
                return null;
            }
            expectedValues[index] = args.get(index).getExpectedValue();
        }
        return func.apply(expectedValues);
    }

    public static class OxlaFunction implements OxlaExpression {
        public final String textRepresentation;
        public final OxlaTypeOverload overload;
        private final OxlaApplyFunction applyFunction;

        public OxlaFunction(String textRepresentation, OxlaTypeOverload overload, OxlaApplyFunction applyFunction) {
            this.textRepresentation = textRepresentation;
            this.overload = overload;
            this.applyFunction = applyFunction;
        }

        public OxlaConstant apply(OxlaConstant[] constants) {
            if (applyFunction == null) {
                // NOTE: `applyFunction` is not implemented, thus PQS oracle should ignore this operator.
                throw new IgnoreMeException();
            }
            if (constants.length != overload.inputTypes.length) {
                throw new AssertionError(String.format("OxlaFunction::apply* failed: expected %d arguments, but got %d", overload.inputTypes.length, constants.length));
            }
            return applyFunction.apply(constants);
        }

        @Override
        public String toString() {
            return textRepresentation;
        }
    }

    public static List<OxlaFunction> MATH = OxlaFunctionBuilder.create()
            .addOneParamMatchReturnOverloads("abs", OxlaDataType.NUMERIC, OxlaFunctionOperation::applyAbs)
            .addOneParamMatchReturnOverloads("cbrt", OxlaDataType.FLOATING_POINT, OxlaFunctionOperation::applyCbrt)
            .addOneParamMatchReturnOverloads("ceil", OxlaDataType.FLOATING_POINT, OxlaFunctionOperation::applyCeil)
            .addOneParamMatchReturnOverloads("ceiling", OxlaDataType.FLOATING_POINT, OxlaFunctionOperation::applyCeil)
            .addOneParamMatchReturnOverload("degrees", OxlaDataType.FLOAT64, constants -> {
                OxlaConstant.OxlaFloat64Constant constant = (OxlaConstant.OxlaFloat64Constant) constants[0];
                return OxlaConstant.createFloat64Constant(Math.toDegrees(constant.value));
            })
            .addOneParamMatchReturnOverload("exp", OxlaDataType.FLOAT64, null)
            .addOneParamMatchReturnOverloads("floor", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("round", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("sin", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("sind", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("asind", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("cos", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("cosd", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("acos", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("cot", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("cotd", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("acosd", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("radians", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("sqrt", OxlaDataType.FLOATING_POINT, OxlaFunctionOperation::applySqrt)
            .addOneParamMatchReturnOverloads("ln", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("log10", OxlaDataType.FLOATING_POINT, null)
            .addMultipleParamOverload("log", new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32, null)
            .addMultipleParamOverload("log", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addOneParamMatchReturnOverloads("log", OxlaDataType.FLOATING_POINT, null)
            .addMultipleParamOverload("atan2", new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32, null)
            .addMultipleParamOverload("atan2", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("atan2d", new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32, null)
            .addMultipleParamOverload("atan2d", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addOneParamMatchReturnOverloads("atan", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("atan2d", OxlaDataType.FLOATING_POINT, null)
            .addNoParamOverload("pi", OxlaDataType.FLOAT64, (ignored) -> OxlaConstant.createFloat64Constant(Math.PI))
            .addOneParamMatchReturnOverloads("tan", OxlaDataType.FLOATING_POINT, null)
            .addOneParamMatchReturnOverloads("tand", OxlaDataType.FLOATING_POINT, null)
            .addMultipleParamOverload("power", new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32, OxlaFunctionOperation::applyBitPower)
            .addMultipleParamOverload("power", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, OxlaFunctionOperation::applyBitPower)
            .addMultipleParamOverload("power", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32, OxlaFunctionOperation::applyBitPower)
            .addMultipleParamOverload("power", new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64, OxlaFunctionOperation::applyBitPower)
            .addOneParamMatchReturnOverloads("sign", OxlaDataType.NUMERIC, null)
            .build();

    public static final List<OxlaFunction> STRING = OxlaFunctionBuilder.create()
            .addMultipleParamOverload("concat", OxlaDataType.ALL, OxlaDataType.TEXT, null)
            .addOneParamOverload("length", OxlaDataType.TEXT, OxlaDataType.INT32, (constants -> OxlaConstant.createInt32Constant(((OxlaConstant.OxlaTextConstant) constants[0]).value.length())))
            .addOneParamOverload("lower", OxlaDataType.TEXT, OxlaDataType.TEXT, constants -> OxlaConstant.createTextConstant(((OxlaConstant.OxlaTextConstant) constants[0]).value.toLowerCase()))
            .addOneParamOverload("upper", OxlaDataType.TEXT, OxlaDataType.TEXT, constants -> OxlaConstant.createTextConstant(((OxlaConstant.OxlaTextConstant) constants[0]).value.toUpperCase()))
            .addMultipleParamOverload("replace", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.TEXT, constants -> {
                final OxlaConstant.OxlaTextConstant base = (OxlaConstant.OxlaTextConstant) constants[0];
                final OxlaConstant.OxlaTextConstant from = (OxlaConstant.OxlaTextConstant) constants[1];
                final OxlaConstant.OxlaTextConstant to = (OxlaConstant.OxlaTextConstant) constants[2];
                return OxlaConstant.createTextConstant(base.value.replace(from.value, to.value));
            })
            .addMultipleParamOverload("starts_with", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN, constants -> {
                final OxlaConstant.OxlaTextConstant base = (OxlaConstant.OxlaTextConstant) constants[0];
                final OxlaConstant.OxlaTextConstant pattern = (OxlaConstant.OxlaTextConstant) constants[1];
                return OxlaConstant.createBooleanConstant(base.value.startsWith(pattern.value));
            })
            .addMultipleParamOverload("ends_with", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN, constants -> {
                final OxlaConstant.OxlaTextConstant base = (OxlaConstant.OxlaTextConstant) constants[0];
                final OxlaConstant.OxlaTextConstant pattern = (OxlaConstant.OxlaTextConstant) constants[1];
                return OxlaConstant.createBooleanConstant(base.value.endsWith(pattern.value));
            })
            .addMultipleParamOverload("substr", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("substr", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("substring", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("substring", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("strpos", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.INT32, null)
            .addMultipleParamOverload("position", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.INT32, null)
            .addMultipleParamOverload("regexp_replace", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("regexp_replace", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.TEXT, null)
            .build();

    public static final List<OxlaFunction> PG_FUNCTIONS = OxlaFunctionBuilder.create()
            .addNoParamOverload("pg_backend_pid", OxlaDataType.INT32, null)
            .addMultipleParamOverload("pg_get_expr", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("pg_get_expr", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32, OxlaDataType.BOOLEAN}, OxlaDataType.TEXT, null)
            .addOneParamOverloads("pg_total_relation_size", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32}, OxlaDataType.INT64, null)
            .addOneParamOverloads("pg_table_size", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32}, OxlaDataType.INT64, null)
            .addOneParamOverload("pg_encoding_to_char", OxlaDataType.INT32, OxlaDataType.TEXT, null)
            .addOneParamOverload("pg_size_pretty", OxlaDataType.INT64, OxlaDataType.TEXT, null)
            .addOneParamOverload("pg_get_userbyid", OxlaDataType.INT64, OxlaDataType.TEXT, null)
            .addOneParamOverload("pg_get_indexdef", OxlaDataType.INT32, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("pg_get_indexdef", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.BOOLEAN}, OxlaDataType.TEXT, null)
            .addOneParamOverload("pg_table_is_visible", OxlaDataType.INT32, OxlaDataType.BOOLEAN, null)
            .addOneParamOverloads("pg_typeof", OxlaDataType.ALL, OxlaDataType.TEXT, null)
            .addOneParamOverload("pg_get_constraintdef", OxlaDataType.INT32, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("pg_get_constraintdef", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.BOOLEAN}, OxlaDataType.TEXT, null)
            .addOneParamOverload("pg_get_statisticsobjdef_columns", OxlaDataType.INT32, OxlaDataType.TEXT, null)
            .addOneParamOverloads("pg_relation_is_publishable", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.INT32}, OxlaDataType.BOOLEAN, null)
            .build();

    public static final List<OxlaFunction> SYSTEM = OxlaFunctionBuilder.create()
            .addNoParamOverload("current_database", OxlaDataType.TEXT, (ignored) -> OxlaConstant.createTextConstant("oxla"))
            .addNoParamOverload("current_schema", OxlaDataType.TEXT, null)
            .addNoParamOverload("version", OxlaDataType.TEXT, null)
            .addOneParamMatchReturnOverload("quote_ident", OxlaDataType.TEXT, null)
            .addMultipleParamOverload("format_type", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.TEXT, null)
            .addTwoParamMatrixOverloads("to_char", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64, OxlaDataType.INTERVAL, OxlaDataType.FLOAT32, OxlaDataType.FLOAT64, OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, new OxlaDataType[]{OxlaDataType.TEXT}, false, null)
            .addMultipleParamOverload("obj_description", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.TEXT}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("shobj_description", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.TEXT}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("col_description", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("has_schema_privilege", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN, null)
            .addMultipleParamOverload("has_schema_privilege", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN, null)
            .addMultipleParamOverload("has_database_privilege", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN, null)
            .addMultipleParamOverload("has_database_privilege", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN, null)
            .addMultipleParamOverload("has_database_privilege", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN, null)
            .build();

    public static final List<OxlaFunction> MISC = OxlaFunctionBuilder.create()
            .addTwoParamMatrixOverloads("date_trunc", new OxlaDataType[]{OxlaDataType.TEXT}, new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP, OxlaDataType.INTERVAL}, false, null)
            .addMultipleParamOverload("format_timestamp", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TIMESTAMP}, OxlaDataType.TEXT, null)
            .addOneParamOverloads("unix_seconds", OxlaDataType.ANY_TIMESTAMP, OxlaDataType.INT64, null)
            .addOneParamOverloads("unix_millis", OxlaDataType.ANY_TIMESTAMP, OxlaDataType.INT64, null)
            .addOneParamOverloads("unix_micros", OxlaDataType.ANY_TIMESTAMP, OxlaDataType.INT64, null)
            .addOneParamOverload("timestamp_seconds", OxlaDataType.INT64, OxlaDataType.TIMESTAMP, null)
            .addOneParamOverload("timestamp_millis", OxlaDataType.INT64, OxlaDataType.TIMESTAMP, null)
            .addOneParamOverload("timestamp_micros", OxlaDataType.INT64, OxlaDataType.TIMESTAMP, null)
            .addMultipleParamOverload("timestamp_trunc", new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.INT32}, OxlaDataType.TIMESTAMP, null)
            .addMultipleParamOverload("to_timestamp", new OxlaDataType[]{OxlaDataType.FLOAT64}, OxlaDataType.TIMESTAMPTZ, null)
            .addMultipleParamOverload("to_timestamp", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.TIMESTAMPTZ, null)
            .addMultipleParamOverload("make_date", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.DATE, null)
            .addNoParamOverload("make_interval", OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_interval", OxlaDataType.repeatedType(OxlaDataType.INT32, 1), OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_interval", OxlaDataType.repeatedType(OxlaDataType.INT32, 2), OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_interval", OxlaDataType.repeatedType(OxlaDataType.INT32, 3), OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_interval", OxlaDataType.repeatedType(OxlaDataType.INT32, 4), OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_interval", OxlaDataType.repeatedType(OxlaDataType.INT32, 5), OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_interval", OxlaDataType.repeatedType(OxlaDataType.INT32, 6), OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_interval", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.FLOAT64}, OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("make_time", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.FLOAT64}, OxlaDataType.TIME, null)
            .addMultipleParamOverload("make_timestamp", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.FLOAT64}, OxlaDataType.TIMESTAMP, null)
            .addMultipleParamOverload("make_timestamptz", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.FLOAT64}, OxlaDataType.TIMESTAMPTZ, null)
            .addMultipleParamOverload("make_timestamptz", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.INT32, OxlaDataType.FLOAT64, OxlaDataType.TEXT}, OxlaDataType.TIMESTAMPTZ, null)
            .addOneParamOverloads("date", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.DATE, null)
            .addMultipleParamOverload("json_extract_path", new OxlaDataType[]{OxlaDataType.JSON, OxlaDataType.TEXT}, OxlaDataType.JSON, null)
            .addMultipleParamOverload("json_extract_path_text", new OxlaDataType[]{OxlaDataType.JSON, OxlaDataType.TEXT}, OxlaDataType.TEXT, null)
            .addOneParamOverload("json_array_length", OxlaDataType.JSON, OxlaDataType.INT32, null)
            .addTwoParamMatrixOverloads("json_array_extract", new OxlaDataType[]{OxlaDataType.JSON}, new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, true, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.DATE, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.JSON, OxlaDataType.JSON}, OxlaDataType.JSON, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.TIME, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.TIMESTAMP, null)
            .addMultipleParamOverload("if", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.TIMESTAMPTZ, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.DATE, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.INTERVAL, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.JSON, OxlaDataType.JSON}, OxlaDataType.JSON, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.TEXT, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.TIME, null)
            .addMultipleParamOverload("nullif", new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.TIMESTAMP, null)
            .build();

    public static final List<OxlaFunction> WINDOW = OxlaFunctionBuilder.create()
            .addNoParamOverload("row_number", OxlaDataType.INT64, null)
            .addNoParamOverload("rank", OxlaDataType.INT64, null)
            .addNoParamOverload("dense_rank", OxlaDataType.INT64, null)
            .addNoParamOverload("percent_rank", OxlaDataType.FLOAT64, null)
            .addNoParamOverload("cume_dist", OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("ntile", new OxlaDataType[]{OxlaDataType.INT32}, OxlaDataType.INT32, null)
            .addOneParamMatchReturnOverloads("lag", OxlaDataType.ALL, null)
            .addTwoParamMatrixOverloads("lag", OxlaDataType.ALL, new OxlaDataType[]{OxlaDataType.INT32}, true, null)
            .addOneParamMatchReturnOverloads("lead", OxlaDataType.ALL, null)
            .addTwoParamMatrixOverloads("lead", OxlaDataType.ALL, new OxlaDataType[]{OxlaDataType.INT32}, true, null)
            .addOneParamMatchReturnOverloads("first_value", OxlaDataType.ALL, null)
            .addOneParamMatchReturnOverloads("last_value", OxlaDataType.ALL, null)
            .addTwoParamMatrixOverloads("nth_value", OxlaDataType.ALL, new OxlaDataType[]{OxlaDataType.INT32}, true, null)
// TODO: Find a way of generating overloads based on constraints and implement these functions:
//       new OxlaFunction("lag", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.ALL(v), OxlaDataType.INT32, OxlaDataType.ALL(v)}, OxlaDataType.ALL(v)), null),
//       new OxlaFunction("lead", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.ALL(v), OxlaDataType.INT32, OxlaDataType.ALL(v)}, OxlaDataType.ALL(v)), null),
            .build();


    public static final List<OxlaFunction> AGGREGATE = OxlaFunctionBuilder.create()
            .addOneParamMatchReturnOverloads("sum", new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64, OxlaDataType.INT32, OxlaDataType.INT64, OxlaDataType.INTERVAL, OxlaDataType.TIME}, null)
            .addOneParamMatchReturnOverloads("min", new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.FLOAT32, OxlaDataType.FLOAT64, OxlaDataType.INT32, OxlaDataType.INT64, OxlaDataType.INTERVAL, OxlaDataType.TEXT, OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP, OxlaDataType.TIME}, null)
            .addOneParamMatchReturnOverloads("max", new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.FLOAT32, OxlaDataType.FLOAT64, OxlaDataType.INT32, OxlaDataType.INT64, OxlaDataType.INTERVAL, OxlaDataType.TEXT, OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP, OxlaDataType.TIME}, null)
            .addOneParamOverloads("avg", new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64, OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.FLOAT64, null)
            .addOneParamMatchReturnOverload("avg", OxlaDataType.INTERVAL, null)
            .addOneParamMatchReturnOverload("avg", OxlaDataType.TIME, null)
            .addOneParamOverloads("count", OxlaDataType.ALL, OxlaDataType.INT64, null)
            .addOneParamMatchReturnOverloads("bool_and", new OxlaDataType[]{OxlaDataType.BOOLEAN}, null)
            .addOneParamMatchReturnOverloads("bool_or", new OxlaDataType[]{OxlaDataType.BOOLEAN}, null)
            .addOneParamMatchReturnOverloads("mode", new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.DATE, OxlaDataType.FLOAT32, OxlaDataType.FLOAT64, OxlaDataType.INT32, OxlaDataType.INT64, OxlaDataType.TEXT, OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP, OxlaDataType.TIME}, null)
            .addTwoParamMatrixOverloads("percentile_disc", new OxlaDataType[]{OxlaDataType.FLOAT64}, OxlaDataType.COMPARABLE_WITHOUT_INTERVAL, false, null)
            .addTwoParamMatrixOverloads("percentile_cont", new OxlaDataType[]{OxlaDataType.FLOAT64}, OxlaDataType.NUMERIC, true, null)
            .addTwoParamMatrixOverloads("for_min", OxlaDataType.AGGREGABLE, OxlaDataType.ALL, false, null)
            .addTwoParamMatrixOverloads("for_max", OxlaDataType.AGGREGABLE, OxlaDataType.ALL, false, null)
            .addMultipleParamOverload("corr", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("covar_pop", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("covar_samp", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_avgx", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_avgy", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_count", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_intercept", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_r2", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_slope", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_sxx", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_sxy", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addMultipleParamOverload("regr_syy", new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64, null)
            .addOneParamOverloads("stddev", OxlaDataType.NUMERIC, OxlaDataType.FLOAT64, null)
            .addOneParamOverloads("stddev_pop", OxlaDataType.NUMERIC, OxlaDataType.FLOAT64, null)
            .addOneParamOverloads("stddev_samp", OxlaDataType.NUMERIC, OxlaDataType.FLOAT64, null)
            .addOneParamOverloads("var_pop", OxlaDataType.NUMERIC, OxlaDataType.FLOAT64, null)
            .addOneParamOverloads("var_samp", OxlaDataType.NUMERIC, OxlaDataType.FLOAT64, null)
            .addOneParamOverloads("variance", OxlaDataType.NUMERIC, OxlaDataType.FLOAT64, null)
            .build();

    public static class OxlaFunctionBuilder {
        private final List<OxlaFunction> overloads = new ArrayList<>();

        public static OxlaFunctionBuilder create() {
            return new OxlaFunctionBuilder();
        }

        public OxlaFunctionBuilder addNoParamOverload(String textRepresentation, OxlaDataType returnType, OxlaApplyFunction applyFunction) {
            overloads.add(new OxlaFunction(textRepresentation, new OxlaTypeOverload(new OxlaDataType[]{}, returnType), applyFunction));
            return this;
        }

        public OxlaFunctionBuilder addMultipleParamOverload(String textRepresentation, OxlaDataType[] inputParams, OxlaDataType returnType, OxlaApplyFunction applyFunction) {
            overloads.add(new OxlaFunction(textRepresentation, new OxlaTypeOverload(inputParams, returnType), applyFunction));
            return this;
        }

        public OxlaFunctionBuilder addOneParamOverload(String textRepresentation, OxlaDataType inputParam, OxlaDataType returnType, OxlaApplyFunction applyFunction) {
            overloads.add(new OxlaFunction(textRepresentation, new OxlaTypeOverload(new OxlaDataType[]{inputParam}, returnType), applyFunction));
            return this;
        }

        public OxlaFunctionBuilder addOneParamOverloads(String textRepresentation, OxlaDataType[] types, OxlaDataType returnType, OxlaApplyFunction applyFunction) {
            for (OxlaDataType type : types) {
                overloads.add(new OxlaFunction(textRepresentation, new OxlaTypeOverload(new OxlaDataType[]{type}, returnType), applyFunction));
            }
            return this;
        }

        public OxlaFunctionBuilder addOneParamMatchReturnOverloads(String textRepresentation, OxlaDataType[] types, OxlaApplyFunction applyFunction) {
            for (OxlaDataType type : types) {
                overloads.add(new OxlaFunction(textRepresentation, new OxlaTypeOverload(new OxlaDataType[]{type}, type), applyFunction));
            }
            return this;
        }

        public OxlaFunctionBuilder addOneParamMatchReturnOverload(String textRepresentation, OxlaDataType type, OxlaApplyFunction applyFunction) {
            overloads.add(new OxlaFunction(textRepresentation, new OxlaTypeOverload(new OxlaDataType[]{type}, type), applyFunction));
            return this;
        }

        public OxlaFunctionBuilder addTwoParamMatrixOverloads(String textRepresentation, OxlaDataType[] firstParam, OxlaDataType[] secondParam, boolean isFirstParamReturnType, OxlaApplyFunction applyFunction) {
            for (OxlaDataType firstType : firstParam) {
                for (OxlaDataType secondType : secondParam) {
                    OxlaDataType returnType = isFirstParamReturnType ? firstType : secondType;
                    OxlaTypeOverload overload = new OxlaTypeOverload(new OxlaDataType[]{firstType, secondType}, returnType);
                    overloads.add(new OxlaFunction(textRepresentation, overload, applyFunction));
                }
            }
            return this;
        }

        public List<OxlaFunction> build() {
            return List.of(overloads.toArray(OxlaFunction[]::new));
        }
    }

    private static OxlaConstant applyAbs(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(Math.abs(((OxlaConstant.OxlaFloat32Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.abs(((OxlaConstant.OxlaFloat64Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaIntegerConstant) {
            return OxlaConstant.createInt64Constant(Math.abs(((OxlaConstant.OxlaIntegerConstant) constant).value));
        }
        throw new AssertionError(String.format("OxlaFunctionOperation::applyAbs failed: %s", constant.getClass()));
    }

    private static OxlaConstant applySqrt(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaNullConstant) {
            return OxlaConstant.createNullConstant();
        } else if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant((float) Math.sqrt(((OxlaConstant.OxlaFloat32Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.sqrt(((OxlaConstant.OxlaFloat64Constant) constant).value));
        }
        throw new AssertionError(String.format("OxlaFunctionOperation::applySqrt failed: %s", constant.getClass()));
    }

    private static OxlaConstant applyCbrt(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaNullConstant) {
            return OxlaConstant.createNullConstant();
        } else if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant((float) Math.cbrt(((OxlaConstant.OxlaFloat32Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.cbrt(((OxlaConstant.OxlaFloat64Constant) constant).value));
        }
        throw new AssertionError(String.format("OxlaFunctionOperation::applyCbrt failed: %s", constant.getClass()));
    }

    private static OxlaConstant applyCeil(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaNullConstant) {
            return OxlaConstant.createNullConstant();
        } else if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant((float) Math.ceil(((OxlaConstant.OxlaFloat32Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.ceil(((OxlaConstant.OxlaFloat64Constant) constant).value));
        }
        throw new AssertionError(String.format("OxlaFunctionOperation::applyCbrt failed: %s", constant.getClass()));
    }

    private static OxlaConstant applyBitPower(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaIntegerConstant && right instanceof OxlaConstant.OxlaIntegerConstant) {
            return OxlaConstant.createInt64Constant((long) Math.pow(((OxlaConstant.OxlaIntegerConstant) left).value, ((OxlaConstant.OxlaIntegerConstant) right).value));
        } else if (left instanceof OxlaConstant.OxlaFloat32Constant && right instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant((float) Math.pow(((OxlaConstant.OxlaFloat32Constant) left).value, ((OxlaConstant.OxlaFloat32Constant) right).value));
        } else if (left instanceof OxlaConstant.OxlaFloat64Constant && right instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.pow(((OxlaConstant.OxlaFloat64Constant) left).value, ((OxlaConstant.OxlaFloat64Constant) right).value));
        }
        throw new AssertionError(String.format("OxlaFunctionOperation::applyBitPower failed: %s vs %s", constants[0].getClass(), constants[1].getClass()));
    }
}
