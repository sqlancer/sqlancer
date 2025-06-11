package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.OxlaColumnReference;
import sqlancer.oxla.ast.OxlaConstant;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaTable;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OxlaInsertIntoGenerator extends OxlaQueryGenerator {
    private static final List<String> errors = List.of(
            "Could not translate expression because: unsupported expression type 26",
            "syntax error, unexpected CAST",
            "syntax error, unexpected POSTGRES_CAST",
            "INSERT has more expressions than target columns",
            "INSERT has more target columns than expressions"
    );
    private static final List<Pattern> regexErrors = List.of(
            Pattern.compile("Attempted operation INSERT encountered invalid data in column\\s+(.*)"),
            Pattern.compile("null value in column \"[^\"]*\" of relation \"[^\"]*\" violates not-null constraint"),
            Pattern.compile("invalid input syntax for type timestamp:\\s+\"[^\"]*\""),
            Pattern.compile("Incorrect number of literals, \\d+ columns were selected, but \\d+ literals were passed."),
            Pattern.compile("cannot implicitly cast from .*?(?=to)to (.*)")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    public OxlaInsertIntoGenerator() {
    }

    public SQLQueryAdapter getQueryForTable(OxlaGlobalState globalState, OxlaTable table) {
        return new SQLQueryAdapter(simpleRule(globalState, new StringBuilder(), table), expectedErrors);
    }

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        final OxlaTable table = globalState.getSchema().getRandomTable();
        final StringBuilder queryBuilder = new StringBuilder();

        enum Rule {SIMPLE, SELECT}
        final String query = switch (Randomly.fromOptions(Rule.values())) {
            case SIMPLE -> simpleRule(globalState, queryBuilder, table);
            case SELECT -> selectRule(globalState, queryBuilder, table, depth + 1);
        };
        return new SQLQueryAdapter(query, expectedErrors);
    }

    private String simpleRule(OxlaGlobalState globalState, StringBuilder queryBuilder, OxlaTable table) {
        final int minRowCount = globalState.getDbmsSpecificOptions().minRowCount;
        final int maxRowCount = globalState.getDbmsSpecificOptions().maxRowCount;
        final int rowCount = globalState.getRandomly().getInteger(minRowCount, maxRowCount + 1); // [)
        queryBuilder.append("INSERT INTO ")
                .append(table.getName())
                .append(' ');

        final var randomColumns = Randomly.nonEmptySubset(table.getColumns());
        final var useSubsetOfColumns = Randomly.getBoolean();
        if (useSubsetOfColumns) {
            queryBuilder
                    .append('(')
                    .append(randomColumns
                            .stream()
                            .map(OxlaColumnReference::new)
                            .map(OxlaColumnReference::getColumn)
                            .map(OxlaColumn::getName)
                            .collect(Collectors.joining(", ")))
                    .append(") ");
        }

        queryBuilder.append("VALUES ");
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            queryBuilder.append('(');
            final var columns = useSubsetOfColumns ? randomColumns : table.getColumns();
            for (int columnIndex = 0; columnIndex < columns.size(); ++columnIndex) {
                // FIXME: Replace with generateExpression after supporting this in Oxla.
                queryBuilder.append(OxlaConstant
                        .getRandomForType(globalState, columns.get(columnIndex).getType())
                        .asPlainLiteral());
                if (columnIndex + 1 != columns.size()) {
                    queryBuilder.append(',');
                }
            }
            queryBuilder.append(')');
            if (rowIndex + 1 != rowCount) {
                queryBuilder.append(',');
            }
        }

        return queryBuilder.toString();
    }

    private String selectRule(OxlaGlobalState globalState, StringBuilder queryBuilder, OxlaTable table, int depth) {
        queryBuilder.append("INSERT INTO ")
                .append(table.getName())
                .append(' ');

        final List<OxlaColumn> randomColumns = Randomly.nonEmptySubset(table.getColumns());
        if (Randomly.getBoolean()) {
            queryBuilder
                    .append('(')
                    .append(randomColumns
                            .stream()
                            .map(OxlaColumnReference::new)
                            .map(OxlaColumnReference::getColumn)
                            .map(OxlaColumn::getName)
                            .collect(Collectors.joining(", ")))
                    .append(") ");
        }
        queryBuilder.append(OxlaSelectGenerator.generate(globalState, depth + 1));
        return queryBuilder.toString();
    }
}
