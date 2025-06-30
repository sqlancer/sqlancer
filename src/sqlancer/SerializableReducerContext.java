package sqlancer;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.Map;

public class SerializableReducerContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private String errorType; // "Exception" or "Oracle"
    private String providerClassName;
    private String dbmsName;
    private String databaseName;
    private List<String> sqlStatements;
    private Set<String> expectedErrors;

    // for Exception type
    private String errorMessage;

    // for Oracle type
    private String oracleType; // "NoREC", "TLPWhere", etc.
    private Map<String, String> oracleQueries;
    private Map<String, Object> oracleOptions;

    public SerializableReducerContext() {
    }

    public SerializableReducerContext(String errorType, String providerClassName, String dbmsName, String databaseName,
            List<String> sqlStatements, Set<String> expectedErrors) {
        this.errorType = errorType;
        this.providerClassName = providerClassName;
        this.dbmsName = dbmsName;
        this.databaseName = databaseName;
        this.sqlStatements = sqlStatements;
        this.expectedErrors = expectedErrors;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getProviderClassName() {
        return providerClassName;
    }

    public void setProviderClassName(String providerClassName) {
        this.providerClassName = providerClassName;
    }

    public String getDbmsName() {
        return dbmsName;
    }

    public void setDbmsName(String dbmsName) {
        this.dbmsName = dbmsName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<String> getSqlStatements() {
        return sqlStatements;
    }

    public void setSqlStatements(List<String> sqlStatements) {
        this.sqlStatements = sqlStatements;
    }

    public Set<String> getExpectedErrors() {
        return expectedErrors;
    }

    public void setExpectedErrors(Set<String> expectedErrors) {
        this.expectedErrors = expectedErrors;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getOracleType() {
        return oracleType;
    }

    public void setOracleType(String oracleType) {
        this.oracleType = oracleType;
    }

    public Map<String, String> getOracleQueries() {
        return oracleQueries;
    }

    public void setOracleQueries(Map<String, String> oracleQueries) {
        this.oracleQueries = oracleQueries;
    }

    public Map<String, Object> getOracleOptions() {
        return oracleOptions;
    }

    public void setOracleOptions(Map<String, Object> oracleOptions) {
        this.oracleOptions = oracleOptions;
    }

    public String getOracleQuery(String queryType) {
        return oracleQueries != null ? oracleQueries.get(queryType) : null;
    }

    public void setOracleQuery(String queryType, String query) {
        if (oracleQueries == null) {
            oracleQueries = new java.util.HashMap<>();
        }
        oracleQueries.put(queryType, query);
    }

    @SuppressWarnings("unchecked")
    public <T> T getOracleOption(String optionName, Class<T> type) {
        if (oracleOptions == null)
            return null;
        Object value = oracleOptions.get(optionName);
        return type.isInstance(value) ? (T) value : null;
    }

    public void setOracleOption(String optionName, Object value) {
        if (oracleOptions == null) {
            oracleOptions = new java.util.HashMap<>();
        }
        oracleOptions.put(optionName, value);
    }

    public String getOptimizedQueryString() {
        return getOracleQuery("optimized");
    }

    public void setOptimizedQueryString(String query) {
        setOracleQuery("optimized", query);
    }

    public String getUnoptimizedQueryString() {
        return getOracleQuery("unoptimized");
    }

    public void setUnoptimizedQueryString(String query) {
        setOracleQuery("unoptimized", query);
    }

    public String getFirstQueryString() {
        return getOracleQuery("first");
    }

    public void setFirstQueryString(String query) {
        setOracleQuery("first", query);
    }

    public String getSecondQueryString() {
        return getOracleQuery("second");
    }

    public void setSecondQueryString(String query) {
        setOracleQuery("second", query);
    }

    public String getThirdQueryString() {
        return getOracleQuery("third");
    }

    public void setThirdQueryString(String query) {
        setOracleQuery("third", query);
    }

    public String getOriginalQueryString() {
        return getOracleQuery("original");
    }

    public void setOriginalQueryString(String query) {
        setOracleQuery("original", query);
    }

    public boolean isShouldUseAggregate() {
        Boolean value = getOracleOption("shouldUseAggregate", Boolean.class);
        return value != null ? value : false;
    }

    public void setShouldUseAggregate(boolean value) {
        setOracleOption("shouldUseAggregate", value);
    }

    public boolean isOrderBy() {
        Boolean value = getOracleOption("orderBy", Boolean.class);
        return value != null ? value : false;
    }

    public void setOrderBy(boolean value) {
        setOracleOption("orderBy", value);
    }
}