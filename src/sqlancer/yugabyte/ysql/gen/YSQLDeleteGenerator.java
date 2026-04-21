package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLVisitor;

public final class YSQLDeleteGenerator extends AbstractDeleteGenerator {

    private final YSQLGlobalState globalState;

    private YSQLDeleteGenerator(YSQLGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        return new YSQLDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        YSQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        errors.add("violates foreign key constraint");
        errors.add("violates not-null constraint");
        errors.add("could not determine which collation to use for string comparison");
        sb.append("DELETE FROM");
        if (Randomly.getBoolean()) {
            sb.append(" ONLY");
        }
        sb.append(" ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(YSQLVisitor.asString(
                    YSQLExpressionGenerator.generateExpression(globalState, table.getColumns(), YSQLDataType.BOOLEAN)));
        }
        if (Randomly.getBoolean()) {
            sb.append(" RETURNING ");
            sb.append(
                    YSQLVisitor.asString(YSQLExpressionGenerator.generateExpression(globalState, table.getColumns())));
        }
        YSQLErrors.addCommonExpressionErrors(errors);
        errors.add("out of range");
        errors.add("cannot cast");
        errors.add("invalid input syntax for");
        errors.add("division by zero");
    }

}
