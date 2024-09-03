package sqlancer.datafusion.ast;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

public class DataFusionSelect extends SelectBase<DataFusionExpression> implements DataFusionExpression,
        Select<DataFusionJoin, DataFusionExpression, DataFusionTable, DataFusionColumn> {
    public Optional<String> fetchColumnsString = Optional.empty(); // When available, override `fetchColumns` in base
    // class's `Node` representation (for display)
    public DataFusionExpressionGenerator exprGen;

    // Construct a `DataFusionSelect` with random SELECT, FROM, WHERE
    public static DataFusionSelect getRandomSelect(DataFusionGlobalState state) {
        DataFusionSelect randomSelect = new DataFusionSelect();

        // Randomly pick up to 4 tables to select from
        DataFusionSchema schema = state.getSchema(); // schema of all tables
        List<DataFusionTable> allTables = schema.getDatabaseTables();
        List<DataFusionTable> randomTables = Randomly.nonEmptySubset(allTables);
        int maxSize = Randomly.fromOptions(1, 2, 3, 4);
        if (randomTables.size() > maxSize) {
            randomTables = randomTables.subList(0, maxSize);
        }

        // Randomly choose some columns from `randomTables`
        // And generate a random expression which might contain those columns
        List<DataFusionSchema.DataFusionColumn> randomColumns = DataFusionTable.getRandomColumns(randomTables);
        randomSelect.exprGen = new DataFusionExpressionGenerator(state).setColumns(randomColumns);
        DataFusionExpression whereExpr = randomSelect.exprGen
                .generateExpression(DataFusionSchema.DataFusionDataType.BOOLEAN);

        // Constructing result
        List<DataFusionExpression> randomTableNodes = randomTables.stream().map(t -> new DataFusionTableReference(t))
                .collect(Collectors.toList());
        List<DataFusionExpression> randomColumnNodes = randomColumns.stream()
                .map((c) -> new DataFusionColumnReference(c)).collect(Collectors.toList());

        randomSelect.setFetchColumns(randomColumnNodes);
        randomSelect.setFromList(randomTableNodes);
        randomSelect.setWhereClause(whereExpr);

        return randomSelect;
    }

    /*
     * If set fetch columns with string It will override `fetchColumns` in base class when
     * `DataFusionToStringVisitor.asString()` is called
     *
     * This method can be helpful to mutate select in oracle checks: SELECT [expr] ... -> SELECT SUM[expr]
     */
    public void setFetchColumnsString(String selectExpr) {
        this.fetchColumnsString = Optional.of(selectExpr);
    }

    @Override
    public void setJoinClauses(List<DataFusionJoin> joinStatements) {
        List<DataFusionExpression> expressions = joinStatements.stream().map(e -> (DataFusionExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<DataFusionJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (DataFusionJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return DataFusionToStringVisitor.asString(this);
    }
}
