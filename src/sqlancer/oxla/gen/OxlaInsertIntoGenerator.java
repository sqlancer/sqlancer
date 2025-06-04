package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.OxlaColumnReference;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaTable;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// TODO OXLA-8192 WITH clause rule.
public class OxlaInsertIntoGenerator extends OxlaQueryGenerator {
    enum Rule {SIMPLE, SELECT}

    private static final List<String> errors = List.of(
            "Could not translate expression because: unsupported expression type 26",
            "syntax error, unexpected CAST",
            "syntax error, unexpected POSTGRES_CAST"
    );
    private static final List<Pattern> regexErrors = List.of();
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors);

    public OxlaInsertIntoGenerator(OxlaGlobalState globalState) {
        super(globalState);
    }

    public SQLQueryAdapter getQueryForTable(OxlaTable table) {
        return simpleRule(table);
    }

    @Override
    public SQLQueryAdapter getQuery(int depth) {
        final Rule rule = Randomly.fromOptions(Rule.values());
        final OxlaTable table = globalState.getSchema().getRandomTable();
        switch (rule) {
            case SIMPLE:
                return simpleRule(table);
            case SELECT:
                return selectRule(table, depth + 1);
            default:
                throw new AssertionError(rule);
        }
    }

    private SQLQueryAdapter simpleRule(OxlaTable table) {
        final int minRowCount = globalState.getDbmsSpecificOptions().minRowCount;
        final int maxRowCount = globalState.getDbmsSpecificOptions().maxRowCount;
        final int rowCount = globalState.getRandomly().getInteger(minRowCount, maxRowCount + 1); // [)
        final StringBuilder queryBuilder = new StringBuilder();
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
                            .collect(Collectors.joining(",")))
                    .append(") ");
        }

        queryBuilder.append("VALUES ");
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            queryBuilder.append('(');
            for (int columnIndex = 0; columnIndex < randomColumns.size(); ++columnIndex) {
                // FIXME: Replace with generateExpression after supporting this in Oxla.
                queryBuilder.append(generator.generateConstant(randomColumns.get(columnIndex).getType()));
                if (columnIndex + 1 != randomColumns.size()) {
                    queryBuilder.append(',');
                }
            }
            queryBuilder.append(')');
            if (rowIndex + 1 != rowCount) {
                queryBuilder.append(',');
            }
        }

        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private SQLQueryAdapter selectRule(OxlaTable table, int depth) {
        final StringBuilder queryBuilder = new StringBuilder();
        // TODO OXLA-8192: SELECT rule.
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }
}
