package sqlancer.duckdb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBExpression;

public final class DuckDBUpdateGenerator extends AbstractUpdateGenerator<DuckDBColumn> {

    private final DuckDBGlobalState globalState;
    private DuckDBExpressionGenerator gen;

    private DuckDBUpdateGenerator(DuckDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        return new DuckDBUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DuckDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        DuckDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(DuckDBColumn column) {
        Node<DuckDBExpression> expr;
        if (Randomly.getBooleanWithSmallProbability()) {
            expr = gen.generateExpression();
            DuckDBErrors.addExpressionErrors(errors);
        } else {
            expr = gen.generateConstant();
        }
        sb.append(DuckDBToStringVisitor.asString(expr));
    }

}
