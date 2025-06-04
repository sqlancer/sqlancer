package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.OxlaToStringVisitor;
import sqlancer.oxla.ast.OxlaColumnReference;
import sqlancer.oxla.ast.OxlaExpression;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaDataType;
import sqlancer.oxla.schema.OxlaTable;
import sqlancer.oxla.schema.OxlaTables;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// TODO OXLA-8192 Implement ORDER BY / LIMIT rules.
public class OxlaSelectGenerator extends OxlaQueryGenerator {
    private enum Rule {SIMPLE, UNION, INTERSECT, EXCEPT}

    private static final ExpectedErrors errors = OxlaCommon.ALL_ERRORS;

    public OxlaSelectGenerator(OxlaGlobalState globalState) {
        super(globalState);
    }

    public static SQLQueryAdapter generate(OxlaGlobalState globalState, int depth) {
        return new OxlaSelectGenerator(globalState).getQuery(depth);
    }

    @Override
    public SQLQueryAdapter getQuery(int depth) {
        if (depth > globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return simpleRule();
        }
        final Rule rule = Randomly.fromOptions(Rule.values());
        switch (rule) {
            case SIMPLE:
                return simpleRule();
            case UNION:
                return unionRule(depth + 1);
            case INTERSECT:
                return intersectRule(depth + 1);
            case EXCEPT:
                return exceptRule(depth + 1);
            default:
                throw new AssertionError(rule);
        }
    }

    private SQLQueryAdapter simpleRule() {
        final StringBuilder queryBuilder = new StringBuilder()
                .append("SELECT ");

        // TYPE
        if (Randomly.getBoolean()) {
            queryBuilder.append("DISTINCT ");
        }

        // WHAT
        final OxlaTables randomSelectTables = globalState.getSchema().getRandomTableNonEmptyTables();
        generator.setTablesAndColumns(randomSelectTables); // TODO: Separate generator for this?
        List<OxlaExpression> what = new ArrayList<>();
        for (int index = 0; index < Randomly.smallNumber() + 1; ++index) {
            what.add(generator.generateExpression(OxlaDataType.getRandomType()));
        }
        queryBuilder.append(OxlaToStringVisitor.asString(what));

        // INTO
        // TODO OXLA-8192 INTO

        // FROM
        final boolean containsColumns = what.stream().anyMatch(e -> e instanceof OxlaColumnReference);
        if (containsColumns || Randomly.getBoolean()) {
            queryBuilder
                    .append(" FROM ")
                    .append(randomSelectTables
                            .getColumns()
                            .stream()
                            .map(OxlaColumn::getTable)
                            .map(OxlaTable::getName)
                            .collect(Collectors.joining(",")));
        }

        // JOIN
        // TODO OXLA-8192 JOIN

        // WHERE
        if (Randomly.getBoolean()) {
            queryBuilder.append(" WHERE ")
                    .append(OxlaToStringVisitor.asString(generator.generatePredicate()));
        }

        // GROUP BY
        if (Randomly.getBoolean()) {
            final List<OxlaExpression> groupByExpressions = generator.generateExpressions(Randomly.smallNumber() + 1);
            queryBuilder.append(" GROUP BY ").append(OxlaToStringVisitor.asString(groupByExpressions));

            // HAVING
            if (Randomly.getBoolean()) {
                queryBuilder.append(" HAVING ").append(OxlaToStringVisitor.asString(generator.generatePredicate()));
            }
        }

        // WINDOWS
        // TODO OXLA-8192 WINDOWS

        return new SQLQueryAdapter(queryBuilder.toString(), errors);
    }

    private SQLQueryAdapter unionRule(int depth) {
        final String firstSelectQuery = getQuery(depth).getUnterminatedQueryString();
        final String secondSelectQuery = getQuery(depth).getUnterminatedQueryString();
        boolean isUnionAll = Randomly.getBoolean();
        return new SQLQueryAdapter(
                String.format("(%s) UNION%s (%s)", firstSelectQuery, isUnionAll ? " ALL" : "", secondSelectQuery),
                errors);
    }

    private SQLQueryAdapter intersectRule(int depth) {
        final String firstSelectQuery = getQuery(depth).getUnterminatedQueryString();
        final String secondSelectQuery = getQuery(depth).getUnterminatedQueryString();
        return new SQLQueryAdapter(
                String.format("(%s) INTERSECT (%s)", firstSelectQuery, secondSelectQuery),
                errors);
    }

    private SQLQueryAdapter exceptRule(int depth) {
        final String firstSelectQuery = getQuery(depth).getUnterminatedQueryString();
        final String secondSelectQuery = getQuery(depth).getUnterminatedQueryString();
        return new SQLQueryAdapter(
                String.format("(%s) EXCEPT (%s)", firstSelectQuery, secondSelectQuery),
                errors);
    }
}
