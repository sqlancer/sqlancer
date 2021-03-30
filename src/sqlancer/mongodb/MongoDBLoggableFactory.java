package sqlancer.mongodb;

import java.util.Arrays;

import sqlancer.common.log.Loggable;
import sqlancer.common.log.LoggableFactory;
import sqlancer.common.log.LoggedString;
import sqlancer.common.query.Query;

public class MongoDBLoggableFactory extends LoggableFactory {
    @Override
    protected Loggable createLoggable(String input, String suffix) {
        return new LoggedString(input + suffix);
    }

    @Override
    public Query<?> getQueryForStateToReproduce(String queryString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Query<?> commentOutQuery(Query<?> query) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Loggable infoToLoggable(String time, String databaseName, String databaseVersion, long seedValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("// Time: ").append(time).append("\n");
        sb.append("// Database: ").append(databaseName).append("\n");
        sb.append("// Database version: ").append(databaseVersion).append("\n");
        sb.append("// seed value: ").append(seedValue).append("\n");
        return new LoggedString(sb.toString());
    }

    @Override
    public Loggable convertStacktraceToLoggable(Throwable throwable) {
        return new LoggedString(Arrays.toString(throwable.getStackTrace()) + "\n" + throwable.getMessage());
    }
}
