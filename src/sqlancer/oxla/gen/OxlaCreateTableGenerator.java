package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.OxlaConstant;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class OxlaCreateTableGenerator extends OxlaQueryGenerator {
    private static final Collection<String> errors = List.of(
            "CreateStatement.type not supported",
            "select within create is not supported",
            "File type is unknown"
    );
    private static final Collection<Pattern> regexErrors = List.of(
            Pattern.compile("relation \"[^\"]*\" already exists"),
            Pattern.compile("column \"[^\"]*\" has unsupported type"),
            Pattern.compile("syntax error, unexpected\\s+")
    );
    public static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);
    private static final AtomicInteger tableIndex = new AtomicInteger();

    public OxlaCreateTableGenerator() {
    }

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        if (depth > globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return simpleRule(globalState);
        }

        enum Rule {FILE, SIMPLE, SELECT, VIEW}
        return switch (Randomly.fromOptions(Rule.values())) {
            case FILE -> fileRule(globalState);
            case SIMPLE -> simpleRule(globalState);
            case SELECT -> selectRule(globalState, depth + 1);
            case VIEW -> viewRule(globalState, depth + 1);
        };
    }

    @Override
    public boolean modifiesDatabaseState() {
        return true;
    }

    private SQLQueryAdapter fileRule(OxlaGlobalState globalState) {
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE TABLE ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex(globalState)))
                .append(" FROM ")
                .append(OxlaConstant.getRandomForType(globalState, OxlaDataType.TEXT).asPlainLiteral().replace("'", ""))
                .append(" FILE ")
                .append(OxlaConstant.getRandomForType(globalState, OxlaDataType.TEXT).asPlainLiteral());
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private SQLQueryAdapter simpleRule(OxlaGlobalState globalState) {
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE TABLE ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex(globalState)))
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

    private SQLQueryAdapter selectRule(OxlaGlobalState globalState, int depth) {
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE TABLE ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex(globalState)))
                .append(" AS ")
                .append(OxlaSelectGenerator.generate(globalState, depth));
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private SQLQueryAdapter viewRule(OxlaGlobalState globalState, int depth) {
        // FIXME: This rule also takes optional list of columns (literals) matching the count of SELECT statements' WHAT.
        final StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE VIEW ")
                .append(Randomly.getBoolean() ? "IF NOT EXISTS " : "")
                .append(DBMSCommon.createTableName(getTableIndex(globalState)))
                .append("_view")
                .append(" AS ")
                .append(OxlaSelectGenerator.generate(globalState, depth));
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private int getTableIndex(OxlaGlobalState globalState) {
        final int minTableCount = globalState.getDbmsSpecificOptions().minTableCount;
        final int maxTableCount = globalState.getDbmsSpecificOptions().maxTableCount;
        final int maxTables = Math.abs(maxTableCount - minTableCount);
        return tableIndex.incrementAndGet() % maxTables;
    }
}
