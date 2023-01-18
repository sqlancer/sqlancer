package sqlancer.cnosdb.client;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import sqlancer.IgnoreMeException;

import java.io.Reader;
import java.sql.SQLException;
import java.util.Iterator;

public class CnosDBResultSet {
    final private Iterator<CSVRecord> records;
    private CSVRecord next = null;

    public CnosDBResultSet(Reader in) throws Exception {
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build()
                .parse(in);
        this.records = records.iterator();
    }

    public void close() {
    }

    public boolean next() throws SQLException {
        if (records.hasNext()) {
            next = records.next();
            return true;
        }
        return false;
    }

    public int getInt(int i) throws SQLException {
        return Integer.parseInt(next.get(i - 1));
    }

    public String getString(int i) throws SQLException {
        return next.get(i - 1);
    }

    public long getLong(int i) throws SQLException {
        if (next.get(i - 1).isEmpty()) {
            throw new IgnoreMeException();
        }
        return Long.parseLong(next.get(i - 1));
    }

    // public boolean getBool(int i) throws Exception {
    // return Boolean.parseBoolean(getString(i));
    // }

}
