package sqlancer.cnosdb;

import sqlancer.cnosdb.query.CnosDBOtherQuery;
import sqlancer.cnosdb.query.CnosDBQueryAdapter;
import sqlancer.common.log.Loggable;
import sqlancer.common.log.LoggableFactory;
import sqlancer.common.log.LoggedString;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;

import java.io.PrintWriter;
import java.io.StringWriter;

public class CnosDBLoggableFactory extends LoggableFactory {

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
    public CnosDBQueryAdapter getQueryForStateToReproduce(String queryString) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addAll(CnosDBExpectedError.Errors());
        return new CnosDBOtherQuery(queryString, errors);
    }

    @Override
    public CnosDBQueryAdapter commentOutQuery(Query<?> query) {
        String queryString = query.getLogString();
        String newQueryString = "-- " + queryString;
        ExpectedErrors errors = new ExpectedErrors();
        errors.addAll(CnosDBExpectedError.Errors());

        return new CnosDBOtherQuery(newQueryString, errors);
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
