package sqlancer.common.ast.newast;

import java.util.List;

public class NewFunctionNode<T, F> implements Node<T> {

    protected List<T> args;
    protected F func;

    public NewFunctionNode(List<T> args, F func) {
        this.args = args;
        this.func = func;
    }

    public List<T> getArgs() {
        return args;
    }

    public F getFunc() {
        return func;
    }

}
