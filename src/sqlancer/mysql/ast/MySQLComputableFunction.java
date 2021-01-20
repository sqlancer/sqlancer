package sqlancer.mysql.ast;

import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.ast.MySQLCastOperation.CastType;

public class MySQLComputableFunction implements MySQLExpression {

    private final MySQLFunction func;
    private final MySQLExpression[] args;

    public MySQLComputableFunction(MySQLFunction func, MySQLExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public MySQLFunction getFunction() {
        return func;
    }

    public MySQLExpression[] getArguments() {
        return args.clone();
    }

    public enum MySQLFunction {

        // ABS(1, "ABS") {
        // @Override
        // public MySQLConstant apply(MySQLConstant[] args, MySQLExpression[] origArgs) {
        // if (args[0].isNull()) {
        // return MySQLConstant.createNullConstant();
        // }
        // MySQLConstant intVal = args[0].castAs(CastType.SIGNED);
        // return MySQLConstant.createIntConstant(Math.abs(intVal.getInt()));
        // }
        // },
        /**
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#function_bit-count">Bit Functions
         *      and Operators</a>
         */
        BIT_COUNT(1, "BIT_COUNT") {

            @Override
            public MySQLConstant apply(MySQLConstant[] evaluatedArgs, MySQLExpression... args) {
                MySQLConstant arg = evaluatedArgs[0];
                if (arg.isNull()) {
                    return MySQLConstant.createNullConstant();
                } else {
                    long val = arg.castAs(CastType.SIGNED).getInt();
                    return MySQLConstant.createIntConstant(Long.bitCount(val));
                }
            }

        },
        // BENCHMARK(2, "BENCHMARK") {
        //
        // @Override
        // public MySQLConstant apply(MySQLConstant[] evaluatedArgs, MySQLExpression[] args) {
        // if (evaluatedArgs[0].isNull()) {
        // return MySQLConstant.createNullConstant();
        // }
        // if (evaluatedArgs[0].castAs(CastType.SIGNED).getInt() < 0) {
        // return MySQLConstant.createNullConstant();
        // }
        // if (Math.abs(evaluatedArgs[0].castAs(CastType.SIGNED).getInt()) > 10) {
        // throw new IgnoreMeException();
        // }
        // return MySQLConstant.createIntConstant(0);
        // }
        //
        // },
        COALESCE(2, "COALESCE") {

            @Override
            public MySQLConstant apply(MySQLConstant[] args, MySQLExpression... origArgs) {
                MySQLConstant result = MySQLConstant.createNullConstant();
                for (MySQLConstant arg : args) {
                    if (!arg.isNull()) {
                        result = MySQLConstant.createStringConstant(arg.castAsString());
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
        /**
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/control-flow-functions.html#function_if">Flow Control
         *      Functions</a>
         */
        IF(3, "IF") {

            @Override
            public MySQLConstant apply(MySQLConstant[] args, MySQLExpression... origArgs) {
                MySQLConstant cond = args[0];
                MySQLConstant left = args[1];
                MySQLConstant right = args[2];
                MySQLConstant result;
                if (cond.isNull() || !cond.asBooleanNotNull()) {
                    result = right;
                } else {
                    result = left;
                }
                return castToMostGeneralType(result, new MySQLExpression[] { origArgs[1], origArgs[2] });

            }

        },
        /**
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/control-flow-functions.html#function_ifnull">IFNULL</a>
         */
        IFNULL(2, "IFNULL") {

            @Override
            public MySQLConstant apply(MySQLConstant[] args, MySQLExpression... origArgs) {
                MySQLConstant result;
                if (args[0].isNull()) {
                    result = args[1];
                } else {
                    result = args[0];
                }
                return castToMostGeneralType(result, origArgs);
            }

        },
        LEAST(2, "LEAST", true) {

            @Override
            public MySQLConstant apply(MySQLConstant[] evaluatedArgs, MySQLExpression... args) {
                return aggregate(evaluatedArgs, (min, cur) -> cur.isLessThan(min).asBooleanNotNull() ? cur : min);
            }

        },
        GREATEST(2, "GREATEST", true) {
            @Override
            public MySQLConstant apply(MySQLConstant[] evaluatedArgs, MySQLExpression... args) {
                return aggregate(evaluatedArgs, (max, cur) -> cur.isLessThan(max).asBooleanNotNull() ? max : cur);
            }
        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        private static MySQLConstant aggregate(MySQLConstant[] evaluatedArgs, BinaryOperator<MySQLConstant> op) {
            boolean containsNull = Stream.of(evaluatedArgs).anyMatch(arg -> arg.isNull());
            if (containsNull) {
                return MySQLConstant.createNullConstant();
            }
            MySQLConstant least = evaluatedArgs[1];
            for (MySQLConstant arg : evaluatedArgs) {
                MySQLConstant left = castToMostGeneralType(least, evaluatedArgs);
                MySQLConstant right = castToMostGeneralType(arg, evaluatedArgs);
                least = op.apply(right, left);
            }
            return castToMostGeneralType(least, evaluatedArgs);
        }

        MySQLFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        MySQLFunction(int nrArgs, String functionName, boolean variadic) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = variadic;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract MySQLConstant apply(MySQLConstant[] evaluatedArgs, MySQLExpression... args);

        public static MySQLFunction getRandomFunction() {
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
    public MySQLConstant getExpectedValue() {
        MySQLConstant[] constants = new MySQLConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i].getExpectedValue() == null) {
                return null;
            }
        }
        return func.apply(constants, args);
    }

    public static MySQLConstant castToMostGeneralType(MySQLConstant cons, MySQLExpression... typeExpressions) {
        if (cons.isNull()) {
            return cons;
        }
        MySQLDataType type = getMostGeneralType(typeExpressions);
        switch (type) {
        case INT:
            if (cons.isInt()) {
                return cons;
            } else {
                return MySQLConstant.createIntConstant(cons.castAs(CastType.SIGNED).getInt());
            }
        case VARCHAR:
            return MySQLConstant.createStringConstant(cons.castAsString());
        default:
            throw new AssertionError(type);
        }
    }

    public static MySQLDataType getMostGeneralType(MySQLExpression... expressions) {
        MySQLDataType type = null;
        for (MySQLExpression expr : expressions) {
            MySQLDataType exprType;
            if (expr instanceof MySQLColumnReference) {
                exprType = ((MySQLColumnReference) expr).getColumn().getType();
            } else {
                exprType = expr.getExpectedValue().getType();
            }
            if (type == null) {
                type = exprType;
            } else if (exprType == MySQLDataType.VARCHAR) {
                type = MySQLDataType.VARCHAR;
            }

        }
        return type;
    }

}
