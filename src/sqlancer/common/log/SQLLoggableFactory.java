package sqlancer.common.log;

import java.io.PrintWriter;
import java.io.StringWriter;

import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;

public class SQLLoggableFactory extends LoggableFactory {

    @Override
    protected Loggable createLoggable(String input, String suffix) {
        String completeString = input;
        if (!input.endsWith(";")) {
            completeString += ";";
        }
        if (suffix != null && suffix.length() != 0) {
            completeString += suffix;
        }
        return new LoggedString(completeString);
    }

    @Override
    public SQLQueryAdapter getQueryForStateToReproduce(String queryString) {
        return new SQLQueryAdapter(queryString);
    }

    @Override
    public SQLQueryAdapter commentOutQuery(Query<?> query) {
        String queryString = query.getLogString();
        String newQueryString = "-- " + queryString;
        return new SQLQueryAdapter(newQueryString);
    }

    @Override
    protected Loggable infoToLoggable(String time, String databaseName, String databaseVersion, long seedValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Time: " + time + "\n");
        sb.append("-- Database: " + databaseName + "\n");
        sb.append("-- Database version: " + databaseVersion + "\n");
        sb.append("-- seed value: " + seedValue + "\n");
        return new LoggedString(sb.toString());
    }

    @Override
    public Loggable convertStacktraceToLoggable(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return new LoggedString("--" + sw.toString().replace("\n", "\n--"));
    }
}
