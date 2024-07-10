package sqlancer.mariadb.ast;

import java.util.List;

public class MariaDBFunction implements MariaDBExpression {

    private final MariaDBFunctionName func;
    private final List<MariaDBExpression> args;

    public MariaDBFunction(MariaDBFunctionName func, List<MariaDBExpression> args) {
        this.func = func;
        this.args = args;
    }

    public MariaDBFunctionName getFunc() {
        return func;
    }

    public List<MariaDBExpression> getArgs() {
        return args;
    }

}
