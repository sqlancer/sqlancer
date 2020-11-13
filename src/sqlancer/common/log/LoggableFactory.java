package sqlancer.common.log;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import sqlancer.common.query.Query;

public abstract class LoggableFactory {

    public Loggable createLoggableWithNoLinebreak(String input) {
        return createLoggable(input, "");
    }

    public Loggable createLoggable(String input) {
        return createLoggable(input, "\n");
    }

    protected abstract Loggable createLoggable(String input, String suffix);

    public abstract Query getQueryForStateToReproduce(String queryString);

    @Deprecated
    public abstract Query commentOutQuery(Query query);

    public Loggable getInfo(String databaseName, String databaseVersion, long seedValue) {
        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        return infoToLoggable(dateFormat.format(date), databaseName, databaseVersion, seedValue);
    }

    protected abstract Loggable infoToLoggable(String time, String databaseName, String databaseVersion,
            long seedValue);

    public abstract Loggable convertStacktraceToLoggable(Throwable throwable);

}
