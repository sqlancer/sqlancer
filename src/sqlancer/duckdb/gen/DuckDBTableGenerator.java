package sqlancer.duckdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractTableGenerator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBExpression;

public class DuckDBTableGenerator extends AbstractTableGenerator<DuckDBColumn> {

    private DuckDBGlobalState globalState;
    private UntypedExpressionGenerator<DuckDBExpression, DuckDBColumn> gen;

    public DuckDBTableGenerator() {
        this.canAffectSchema = true;
    }

    public SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        this.globalState = globalState;
        return getStatement();
    }

    @Override
    public void buildStatement() {
        String tableName = globalState.getSchema().getFreeTableName();
        appendCreateTable(tableName);
        List<DuckDBColumn> columns = getNewColumns();
        gen = new DuckDBExpressionGenerator(globalState).setColumns(columns);
        sb.append("(");
        appendColumnDefinitionList(columns);
        if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBoolean()) {
            errors.add("Invalid type for index");
            List<DuckDBColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
            sb.append(", PRIMARY KEY(");
            sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(")");
    }

    @Override
    protected void appendColumnDefinition(DuckDBColumn column) {
        sb.append(column.getName());
        sb.append(" ");
        sb.append(column.getType());
        if (globalState.getDbmsSpecificOptions().testCollate && Randomly.getBooleanWithRatherLowProbability()
                && column.getType().getPrimitiveDataType() == DuckDBDataType.VARCHAR) {
            sb.append(" COLLATE ");
            sb.append(getRandomCollate());
        }
        if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" UNIQUE");
        }
        if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                && Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" NOT NULL");
        }
        if (globalState.getDbmsSpecificOptions().testCheckConstraints
                && Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" CHECK(");
            sb.append(DuckDBToStringVisitor.asString(gen.generateExpression()));
            DuckDBErrors.addExpressionErrors(errors);
            sb.append(")");
        }
        if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
            sb.append(" DEFAULT(");
            sb.append(DuckDBToStringVisitor.asString(gen.generateConstant()));
            sb.append(")");
        }
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
    }

    private static List<DuckDBColumn> getNewColumns() {
        List<DuckDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            DuckDBCompositeDataType columnType = DuckDBCompositeDataType.getRandomWithoutNull();
            columns.add(new DuckDBColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
