package sqlancer.influxdb;

import java.util.Arrays;
import java.util.List;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "InfluxDB (default port: " + InfluxDBOptions.DEFAULT_PORT
        + " default host: " + InfluxDBOptions.DEFAULT_HOST + ")")
public class InfluxDBOptions implements DBMSSpecificOptions<InfluxDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8086;

    @Parameter(names = "--oracle")
    public List<InfluxDBOracleFactory> oracle = Arrays.asList(InfluxDBOracleFactory.DEFAULT);

    @Parameter(names = "--username", description = "The user name used to log into InfluxDB")
    //setting the username for login to influxDB
    private String userName = "admin"; // NOPMD
    //set the password for login to influxDB
    @Parameter(names = "--password", description = "The password used to log into InfluxDB")
    private String password = "influx"; // NOPMD

    @Override
    public List<InfluxDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }
}