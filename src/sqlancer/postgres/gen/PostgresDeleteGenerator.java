package sqlancer.postgres.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;

public final class PostgresDeleteGenerator extends AbstractDeleteGenerator {

    private final PostgresGlobalState globalState;

    private PostgresDeleteGenerator(PostgresGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        return new PostgresDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        PostgresTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        errors.add("violates foreign key constraint");
        errors.add("violates not-null constraint");
        errors.add("could not determine which collation to use for string comparison");
        appendDeleteFromTable(table.getName(), Randomly.getBoolean());
        if (Randomly.getBoolean()) {
            appendWhereClause(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState,
                    table.getColumns(), PostgresDataType.BOOLEAN)));
        }
        if (Randomly.getBoolean()) {
            appendReturningClause(PostgresVisitor
                    .asString(PostgresExpressionGenerator.generateExpression(globalState, table.getColumns())));
        }
        PostgresCommon.addCommonExpressionErrors(errors);
        errors.add("out of range");
        errors.add("cannot cast");
        errors.add("invalid input syntax for");
        errors.add("division by zero");
    }

}
