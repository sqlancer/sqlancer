package sqlancer.postgres.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;

/**
 * @see <a href= "https://www.postgresql.org/docs/devel/sql-comment.html">COMMENT</a>
 */
public final class PostgresCommentGenerator {

    private PostgresCommentGenerator() {
    }

    private enum Action {
        INDEX, COLUMN, STATISTICS, TABLE
    }

    public static SQLQueryAdapter generate(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder("COMMENT ON ");
        Action type = Randomly.fromOptions(Action.values());
        PostgresTable randomTable = globalState.getSchema().getRandomTable();

        switch (type) {
        case INDEX:
            appendIndexComment(sb, randomTable);
            break;
        case COLUMN:
            appendColumnComment(sb, randomTable);
            break;
        case STATISTICS:
            appendStatisticsComment(sb, randomTable);
            break;
        case TABLE:
            appendTableComment(sb, randomTable);
            break;
        default:
            throw new AssertionError(type);
        }

        sb.append(" IS ").append(getCommentValue(globalState));
        return new SQLQueryAdapter(sb.toString());
    }

    private static void appendIndexComment(StringBuilder sb, PostgresTable table) {
        sb.append("INDEX ");
        if (table.getIndexes().isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append(table.getRandomIndex().getIndexName());
    }

    private static void appendColumnComment(StringBuilder sb, PostgresTable table) {
        sb.append("COLUMN ").append(table.getRandomColumn().getFullQualifiedName());
    }

    private static void appendStatisticsComment(StringBuilder sb, PostgresTable table) {
        sb.append("STATISTICS ");
        if (table.getStatistics().isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append(table.getStatistics().get(0).getName());
    }

    private static void appendTableComment(StringBuilder sb, PostgresTable table) {
        sb.append("TABLE ");
        if (table.isView()) {
            throw new IgnoreMeException();
        }
        sb.append(table.getName());
    }

    private static String getCommentValue(PostgresGlobalState globalState) {
        return Randomly.getBoolean() ? "NULL" : "'" + globalState.getRandomly().getString().replace("'", "''") + "'";
    }
}
