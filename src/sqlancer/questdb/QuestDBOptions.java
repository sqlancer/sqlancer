package sqlancer.questdb;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "QuestDB (default port: " + QuestDBOptions.DEFAULT_PORT
        + " default host: " + QuestDBOptions.DEFAULT_HOST + ")")
public class QuestDBOptions implements DBMSSpecificOptions<QuestDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8812;

    @Parameter(names = "--oracle")
    public List<QuestDBOracleFactory> oracle = Arrays.asList(QuestDBOracleFactory.WHERE);

    @Parameter(names = "--username", description = "The user name used to log into QuestDB")
    private String userName = "admin"; // NOPMD

    @Parameter(names = "--password", description = "The password used to log into QuestDB")
    private String password = "quest"; // NOPMD

    @Override
    public List<QuestDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

}
