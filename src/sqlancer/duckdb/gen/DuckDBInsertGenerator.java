package sqlancer.duckdb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBInsertGenerator extends AbstractInsertGenerator<DuckDBColumn> {

    private final DuckDBGlobalState globalState;

    public DuckDBInsertGenerator(DuckDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        return new DuckDBInsertGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DuckDBColumn> columns = table.getRandomNonEmptyColumnSubsetFilter(p -> !p.getName().equals("rowid"));
        buildInsertInto(table.getName(), columns);
        DuckDBErrors.addInsertErrors(errors);
    }

    @Override
    protected void insertValue(DuckDBColumn columnDuckDB) {
        // TODO: select a more meaningful value
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("DEFAULT");
        } else {
            sb.append(DuckDBToStringVisitor.asString(new DuckDBExpressionGenerator(globalState).generateConstant()));
        }
    }

}
