package sqlancer;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

public class ReducerContext implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum ErrorType {
        EXCEPTION, ORACLE
    }

    public enum OracleType {
        NOREC, TLP_WHERE
    }

    private ErrorType errorType;
    private String providerClassName;
    private String dbmsName;
    private String databaseName;
    private List<String> sqlStatements;
    private Set<String> expectedErrors;

    private String errorMessage; // for Exception type
    private OracleType oracleType; // for Oracle type
    private Map<String, String> reproducerData;

    public ReducerContext() {
        this.reproducerData = new HashMap<>();
    }

    public ReducerContext(ErrorType errorType, String providerClassName, String dbmsName, String databaseName,
            List<String> sqlStatements, Set<String> expectedErrors) {
        this.errorType = errorType;
        this.providerClassName = providerClassName;
        this.dbmsName = dbmsName;
        this.databaseName = databaseName;
        this.sqlStatements = sqlStatements;
        this.expectedErrors = expectedErrors;
        this.reproducerData = new HashMap<>();
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public String getProviderClassName() {
        return providerClassName;
    }

    public String getDbmsName() {
        return dbmsName;
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

    public Set<String> getExpectedErrors() {
        return expectedErrors;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public OracleType getOracleType() {
        return oracleType;
    }

    public void setOracleType(OracleType oracleType) {
        this.oracleType = oracleType;
    }

    public Map<String, String> getReproducerData() {
        return reproducerData;
    }

    public void setReproducerData(Map<String, String> reproducerData) {
        this.reproducerData = reproducerData;
    }
}