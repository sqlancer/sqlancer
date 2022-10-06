package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLIndex;

public final class YSQLReindexGenerator {

    private YSQLReindexGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("could not create unique index"); // CONCURRENT INDEX
        StringBuilder sb = new StringBuilder();
        sb.append("REINDEX");
        // if (Randomly.getBoolean()) {
        // sb.append(" VERBOSE");
        // }
        sb.append(" ");
        Scope scope = Randomly.fromOptions(Scope.values());
        switch (scope) {
        case INDEX:
            sb.append("INDEX ");
            List<YSQLIndex> indexes = globalState.getSchema().getRandomTable().getIndexes();
            if (indexes.isEmpty()) {
                throw new IgnoreMeException();
            }
            sb.append(indexes.stream().map(YSQLIndex::getIndexName).collect(Collectors.joining()));
            break;
        case TABLE:
            sb.append("TABLE ");
            sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
            break;
        case DATABASE:
            sb.append("DATABASE ");
            sb.append(globalState.getSchema().getDatabaseName());
            break;
        default:
            throw new AssertionError(scope);
        }
        errors.add("already contains data"); // FIXME bug report
        errors.add("does not exist"); // internal index
        errors.add("REINDEX is not yet implemented for partitioned indexes");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private enum Scope {
        INDEX, TABLE, DATABASE
    }

}
