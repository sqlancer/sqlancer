package sqlancer;

import java.sql.Connection;

import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;

public class GlobalState<O> {

    private Connection con;
    private Randomly r;
    private MainOptions options;
    private O dmbsSpecificOptions;
    private StateLogger logger;
    private StateToReproduce state;
    private QueryManager manager;
    private String databaseName;

    public void setConnection(Connection con) {
        this.con = con;
    }

    @SuppressWarnings("unchecked")
    public void setDmbsSpecificOptions(Object dmbsSpecificOptions) {
        this.dmbsSpecificOptions = (O) dmbsSpecificOptions;
    }

    public O getDmbsSpecificOptions() {
        return dmbsSpecificOptions;
    }

    public Connection getConnection() {
        return con;
    }

    public void setRandomly(Randomly r) {
        this.r = r;
    }

    public Randomly getRandomly() {
        return r;
    }

    public MainOptions getOptions() {
        return options;
    }

    public void setMainOptions(MainOptions options) {
        this.options = options;
    }

    public void setStateLogger(StateLogger logger) {
        this.logger = logger;
    }

    public StateLogger getLogger() {
        return logger;
    }

    public void setState(StateToReproduce state) {
        this.state = state;
    }

    public StateToReproduce getState() {
        return state;
    }

    public QueryManager getManager() {
        return manager;
    }

    public void setManager(QueryManager manager) {
        this.manager = manager;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

}
