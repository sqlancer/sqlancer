package sqlancer.oceanbase.ast;

import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbase.ast.OceanBaseCastOperation.CastType;

public class OceanBaseComputableFunction implements OceanBaseExpression {

    private final OceanBaseFunction func;
    private final OceanBaseExpression[] args;

    public OceanBaseComputableFunction(OceanBaseFunction func, OceanBaseExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public OceanBaseFunction getFunction() {
        return func;
    }

    public OceanBaseExpression[] getArguments() {
        return args.clone();
    }

    public enum OceanBaseFunction {

        BIT_COUNT(1, "BIT_COUNT") {

            @Override
            public OceanBaseConstant apply(OceanBaseConstant[] evaluatedArgs, OceanBaseExpression... args) {
                OceanBaseConstant arg = evaluatedArgs[0];
                if (arg.isNull()) {
                    return OceanBaseConstant.createNullConstant();
                } else {
                    long val = arg.castAs(CastType.SIGNED).getInt();
                    return OceanBaseConstant.createIntConstant(Long.bitCount(val));
                }
            }

        },
        COALESCE(2, "COALESCE") {

            @Override
            public OceanBaseConstant apply(OceanBaseConstant[] args, OceanBaseExpression... origArgs) {
                OceanBaseConstant result = OceanBaseConstant.createNullConstant();
                for (OceanBaseConstant arg : args) {
                    if (!arg.isNull()) {
                        result = arg;
                        break;
                    }
                }
                return castToMostGeneralType(result, origArgs);
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        IF(3, "IF") {

            @Override
            public OceanBaseConstant apply(OceanBaseConstant[] args, OceanBaseExpression... origArgs) {
                OceanBaseConstant cond = args[0];
                OceanBaseConstant left = args[1];
                OceanBaseConstant right = args[2];
                OceanBaseConstant result;
                if (cond.isNull() || !cond.asBooleanNotNull()) {
                    result = right;
                } else {
                    result = left;
                }
                return castToMostGeneralType(result, new OceanBaseExpression[] { origArgs[1], origArgs[2] });
            }
        },

        IFNULL(2, "IFNULL") {

            @Override
            public OceanBaseConstant apply(OceanBaseConstant[] args, OceanBaseExpression... origArgs) {
                OceanBaseConstant result;
                if (args[0].isNull()) {
                    result = args[1];
                } else {
                    result = args[0];
                } // args[0] and args[1] both null, if type is varchar, return null of varchar
                return castToMostGeneralType(result, origArgs);
            }

        },
        LEAST(2, "LEAST", true) {

            @Override
            public OceanBaseConstant apply(OceanBaseConstant[] evaluatedArgs, OceanBaseExpression... args) {
                return aggregate(evaluatedArgs, args, (min, cur) -> cur.isLessThan(min).asBooleanNotNull() ? cur : min);
            }

        },
        GREATEST(2, "GREATEST", true) {
            @Override
            public OceanBaseConstant apply(OceanBaseConstant[] evaluatedArgs, OceanBaseExpression... args) {
                return aggregate(evaluatedArgs, args, (max, cur) -> cur.isLessThan(max).asBooleanNotNull() ? max : cur);
            }
        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        private static OceanBaseConstant aggregate(OceanBaseConstant[] evaluatedArgs,
                OceanBaseExpression[] typeExpressions, BinaryOperator<OceanBaseConstant> op) {
            boolean containsNull = Stream.of(evaluatedArgs).anyMatch(arg -> arg.isNull());
            if (containsNull) {
                // IFNULL(GREATEST('aa',NULL), 0) -> '0'
                // case1:c1 is float，value is NULL;select COALESCE(GREATEST(NULL, concat(t1.c1)), 1) from t1;->'1'
                // select COALESCE(GREATEST(1, concat(t1.c1)), 1) from t1;->1
                // select COALESCE(GREATEST('0', 1, concat(t1.c1)), 1) from t1;->1
                // select COALESCE(GREATEST('0', concat(t1.c1)), 1) from t1;->'1'
                // select COALESCE(GREATEST(NULL, concat(t1.c1)), 1) from t1;->'1'
                // case2: c0 is decimal,value is NULL
                // select IFNULL(GREATEST("iffI|2&nBJLQQ", c0, '0'), 1) from t0;->1
                // select IFNULL(GREATEST("iffI|2&nBJLQQ", NULL, '0'), 1) from t0;->'1'
                OceanBaseDataType type;
                boolean allVarchar = true;
                for (OceanBaseExpression expr : typeExpressions) {
                    if (expr instanceof OceanBaseColumnReference) {
                        type = ((OceanBaseColumnReference) expr).getColumn().getType();
                        if (type == OceanBaseDataType.FLOAT) {
                            type = OceanBaseDataType.VARCHAR;
                        }
                    } else {
                        type = expr.getExpectedValue().getType();
                    }
                    if (type != null && type.isNumeric()) {
                        allVarchar = false;
                        break;
                    }
                }
                if (allVarchar) {
                    return OceanBaseConstant.createStringConstant("null");
                } else {
                    return OceanBaseConstant.createNullConstant();
                }
            }
            OceanBaseConstant least = evaluatedArgs[1];
            /*
             * select least(1,'H8*GPLuBjDj#Xem]W'); -> 0 select least('1','H8*GPLuBjDj#Xem]W'); ->1 select
             * LEAST('000000000001', 'b', 1);->0
             */
            OceanBaseDataType dataType = evaluatedArgs[0].getType();
            boolean sameDataType = true;
            for (OceanBaseConstant arg : evaluatedArgs) {
                if (arg.getType() != dataType) {
                    sameDataType = false;
                    break;
                }
            }
            for (OceanBaseConstant arg : evaluatedArgs) {
                OceanBaseConstant left;
                OceanBaseConstant right;
                if (sameDataType) {
                    left = least;
                    right = arg;
                } else {
                    // select GREATEST('1.47529e18', -1188315266);->1.47529e18
                    if (least.getType() == OceanBaseDataType.VARCHAR) {
                        left = least.castAsDouble();
                    } else {
                        left = least;
                    }
                    if (arg.getType() == OceanBaseDataType.VARCHAR) {
                        right = arg.castAsDouble();
                    } else {
                        right = arg;
                    }
                }
                least = op.apply(right, left);
            }
            return least;
        }

        OceanBaseFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        OceanBaseFunction(int nrArgs, String functionName, boolean variadic) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = variadic;
        }

        public int getNrArgs() {
            return nrArgs;
        }

        public abstract OceanBaseConstant apply(OceanBaseConstant[] evaluatedArgs, OceanBaseExpression... args);

        public static OceanBaseFunction getRandomFunction() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return variadic;
        }

        public String getName() {
            return functionName;
        }
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        OceanBaseConstant[] constants = new OceanBaseConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
        }
        return func.apply(constants, args);
    }

    public static OceanBaseConstant castToMostGeneralType(OceanBaseConstant cons,
            OceanBaseExpression... typeExpressions) {
        OceanBaseDataType type = getMostGeneralType(typeExpressions);
        if (cons.isNull()) {
            if (type == OceanBaseDataType.FLOAT || type == OceanBaseDataType.VARCHAR) {
                return OceanBaseConstant.createStringConstant("null");
            } else {
                return cons;
            }
        } else {
            switch (type) {
            case INT:
                if (cons.isInt()) {
                    return cons;
                } else {
                    return OceanBaseConstant.createIntConstant(cons.castAs(CastType.SIGNED).getInt());
                }
            case VARCHAR:
                return OceanBaseConstant.createStringConstant(cons.castAsString());
            default:
                return cons;
            }
        }
    }

    public static OceanBaseDataType getMostGeneralType(OceanBaseExpression... expressions) {
        OceanBaseDataType type = null;
        for (OceanBaseExpression expr : expressions) {
            OceanBaseDataType exprType;
            if (expr instanceof OceanBaseColumnReference) {
                exprType = ((OceanBaseColumnReference) expr).getColumn().getType();
                if (((OceanBaseColumnReference) expr).getColumn().isZeroFill()) {
                    exprType = OceanBaseDataType.VARCHAR;
                }
            } else {
                exprType = expr.getExpectedValue().getType();
            }
            if (type == null) {
                type = exprType;
                if (exprType == OceanBaseDataType.FLOAT) {
                    type = OceanBaseDataType.VARCHAR;
                }
            } else if (exprType == OceanBaseDataType.VARCHAR || exprType == OceanBaseDataType.FLOAT) {
                type = OceanBaseDataType.VARCHAR;
            }

        }
        return type;
    }

}
