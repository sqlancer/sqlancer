package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3ExpressionFunction implements SQLite3Expression {

    private final SQLite3Expression[] arguments;
    private final String name;

    public SQLite3ExpressionFunction(String name, SQLite3Expression... arguments) {
        this.name = name;
        this.arguments = arguments.clone();
    }

    public SQLite3Expression[] getArguments() {
        return arguments.clone();
    }

    public String getName() {
        return name;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        for (SQLite3Expression arg : arguments) {
            if (arg.getExplicitCollateSequence() != null) {
                return arg.getExplicitCollateSequence();
            }
        }
        return null;
    }

}
