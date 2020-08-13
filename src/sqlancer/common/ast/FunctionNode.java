package sqlancer.common.ast;

import java.util.List;

public abstract class FunctionNode<F, A> {

    protected F function;
    protected List<A> args;

    public FunctionNode(F function, List<A> args) {
        this.function = function;
        this.args = args;
    }

    public F getFunction() {
        return function;
    }

    public List<A> getArgs() {
        return args;
    }
}
