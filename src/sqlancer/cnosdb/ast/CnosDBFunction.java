package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBFunction implements CnosDBExpression {

    private final String func;
    private final CnosDBExpression[] args;
    private final CnosDBDataType returnType;

    public CnosDBFunction(CnosDBFunctionWithUnknownResult f, CnosDBDataType returnType, CnosDBExpression... args) {
        this.func = f.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public String getFunctionName() {
        return func;
    }

    public CnosDBExpression[] getArguments() {
        return args.clone();
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return returnType;
    }

}
