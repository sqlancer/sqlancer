package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class OxlaCreateTableGenerator extends OxlaQueryGenerator {
    enum Rule {FILE, SIMPLE, SELECT, VIEW}

    private static final Collection<String> errors = List.of(
            "CreateStatement.type not supported",
            "select within create is not supported"
    );
    private static final Collection<Pattern> regexErrors = List.of(
            Pattern.compile("relation \"[^\"]*\" already exists"),
            Pattern.compile("column \"[^\"]*\" has unsupported type")
    );
    public static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors);
    private static final AtomicInteger tableIndex = new AtomicInteger();

    public OxlaCreateTableGenerator(OxlaGlobalState globalState) {
        super(globalState);
    }

    @Override
    public SQLQueryAdapter getQuery(int depth) {
        if (depth > globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return simpleRule();
        }
        final Rule rule = Randomly.fromOptions(Rule.values());
        switch (rule) {
            case FILE:
                return fileRule();
            case SIMPLE:
                return simpleRule();
            case SELECT:
                return selectRule(depth + 1);
            case VIEW:
                return viewRule(depth + 1);
            default:
                throw new AssertionError(rule);
        }
    }

    private SQLQueryAdapter fileRule() {
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE TABLE ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex()))
                .append(" FROM ")
                .append(generator.generateExpression(OxlaDataType.TEXT)
                        .toString()
                        .replaceAll("'", "")
                        .replaceAll("TEXT ", ""))
                .append(" FILE ")
                .append(generator.generateExpression(OxlaDataType.TEXT));

        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private SQLQueryAdapter simpleRule() {
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE TABLE ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex()))
                .append('(');

        final int columnCount = Randomly.smallNumber() + 1;
        for (int index = 0; index < columnCount; ++index) {
            queryBuilder.append(DBMSCommon.createColumnName(index))
                    .append(' ')
                    .append(OxlaDataType.getRandomType())
                    .append(' ')
                    .append(Randomly.fromOptions("NOT NULL", "NULL"));
            if (index + 1 < columnCount) {
                queryBuilder.append(',');
            }
        }
        queryBuilder.append(')');

        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private SQLQueryAdapter selectRule(int depth) {
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE TABLE ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex()))
                .append(" AS ")
                .append(OxlaSelectGenerator.generate(globalState, depth));
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private SQLQueryAdapter viewRule(int depth) {
        // FIXME: This rule also takes optional list of columns (literals) matching the count of SELECT statements' WHAT.
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE VIEW ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex()))
                .append("_view")
                .append(" AS ")
                .append(OxlaSelectGenerator.generate(globalState, depth));
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private int getTableIndex() {
        final int minTableCount = globalState.getDbmsSpecificOptions().minTableCount;
        final int maxTableCount = globalState.getDbmsSpecificOptions().maxTableCount;
        final int maxTables = Math.abs(maxTableCount - minTableCount);
        return tableIndex.incrementAndGet() % maxTables;
    }
}
