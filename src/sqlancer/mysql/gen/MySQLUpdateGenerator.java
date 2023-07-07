package sqlancer.mysql.gen;

import java.sql.SQLException;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLVisitor;

public class MySQLUpdateGenerator extends AbstractUpdateGenerator<MySQLColumn> {

    private final MySQLGlobalState globalState;
    private MySQLExpressionGenerator gen;

    public MySQLUpdateGenerator(MySQLGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(MySQLGlobalState globalState) throws SQLException {
        return new MySQLUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() throws SQLException {
        MySQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new MySQLExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            MySQLErrors.addExpressionErrors(errors);
            sb.append(MySQLVisitor.asString(gen.generateExpression()));
        }
        MySQLErrors.addInsertUpdateErrors(errors);
        errors.add("doesn't have this option");

        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(MySQLColumn column) {
        if (Randomly.getBoolean()) {
            sb.append(gen.generateConstant());
        } else if (Randomly.getBoolean()) {
            sb.append("DEFAULT");
        } else {
            sb.append(MySQLVisitor.asString(gen.generateExpression()));
        }
    }

}
