package sqlancer.datafusion;

import static java.lang.System.exit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;

public final class DataFusionUtil {
    private DataFusionUtil() {
        dfAssert(false, "Utility class cannot be instantiated");
    }

    // Display tables in `fromTableNames`
    public static String displayTables(DataFusionGlobalState state, List<String> fromTableNames) {
        StringBuilder resultStringBuilder = new StringBuilder();
        for (String tableName : fromTableNames) {
            String query = String.format("select * from %s", tableName);
            try (Statement stat = state.getConnection().createStatement();
                    ResultSet wholeTable = stat.executeQuery(query)) {

                ResultSetMetaData metaData = wholeTable.getMetaData();
                int columnCount = metaData.getColumnCount();

                resultStringBuilder.append("Table: ").append(tableName).append("\n");
                for (int i = 1; i <= columnCount; i++) {
                    resultStringBuilder.append(metaData.getColumnName(i)).append(" (")
                            .append(metaData.getColumnTypeName(i)).append(")");
                    if (i < columnCount) {
                        resultStringBuilder.append(", ");
                    }
                }
                resultStringBuilder.append("\n");

                while (wholeTable.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        resultStringBuilder.append(wholeTable.getString(i));
                        if (i < columnCount) {
                            resultStringBuilder.append(", ");
                        }
                    }
                    resultStringBuilder.append("\n");
                }
                resultStringBuilder.append("----------------------------------------\n\n");

            } catch (SQLException err) {
                resultStringBuilder.append("Table: ").append(tableName).append("\n");
                resultStringBuilder.append("----------------------------------------\n\n");
                // resultStringBuilder.append("Error retrieving data from table ").append(tableName).append(":
                // ").append(err.getMessage()).append("\n");
            }
        }

        return resultStringBuilder.toString();
    }

    public static void dfAssert(boolean condition, String message) {
        if (!condition) {
            String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
            System.err.println("DataFusion assertion failed in function '" + methodName + "': " + message);
            exit(1);
        }
    }

    /*
     * Fetch all DMLs from logs/database*-cur.log
     */
    public static String getReplay(String dbname) {
        String path = "./logs/datafusion/" + dbname + "-cur.log";
        String absolutePath = Paths.get(path).toAbsolutePath().toString();

        StringBuilder reproducer = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(absolutePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Check if the line contains the /*DML*/ marker
                if (line.contains("/*DML*/")) {
                    reproducer.append(line).append("\n");
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading from file: " + e.getMessage());
        }

        return reproducer.toString();
    }

    // UID for different fuzzer runs
    public static class DataFusionInstanceID {
        private final String id;

        public DataFusionInstanceID(String dfID) {
            id = dfID;
        }

        @Override
        public String toString() {
            return id; // Return the id field when toString is called
        }
    }

    /*
     * Extra logs stored in 'logs/datafusion_custom_log/' In case re-run overwrite previous logs
     */
    public static class DataFusionLogger {
        private final DataFusionInstanceID dfID;
        private final DataFusionGlobalState state;
        /*
         * Log file handles
         */
        private final File errorLogFile;

        public DataFusionLogger(DataFusionGlobalState globalState, DataFusionInstanceID id) throws Exception {
            this.state = globalState;
            this.dfID = id;

            // Setup datafusion_custom_log folder
            File baseDir = new File("logs/datafusion_custom_log/");
            if (!baseDir.exists() && !baseDir.mkdirs()) {
                throw new IOException("Failed to create 'datafusion_custom_log' directory/");
            }

            // Setup error.log
            errorLogFile = new File(baseDir, "error_report.log");
            errorLogFile.createNewFile();
        }

        // Caller is responsible for adding '\n' at the end of logContent
        public void appendToLog(DataFusionLogType logType, String logContent) {
            FileWriter logFileWriter = null;

            // Determine which log file to use based on the LogType
            String logLineHeader = "";
            switch (logType) {
            case ERROR:
                try {
                    logFileWriter = new FileWriter(errorLogFile, true);
                } catch (IOException e) {
                    System.out.println("Failed to create FileWriter for errorLogFIle");
                    e.printStackTrace();
                    exit(1);
                }
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                String formattedDateTime = LocalDateTime.now().format(formatter);
                logLineHeader = "Run@" + formattedDateTime + " (" + dfID + ")\n";
                break;
            case DML:
                logFileWriter = state.getLogger().getCurrentFileWriter();
                logLineHeader = "/*DML*/";
                break;
            case SELECT:
                logFileWriter = state.getLogger().getCurrentFileWriter();
                break;
            default:
                dfAssert(false, "All branch should be covered");
            }

            // Append content to the appropriate log file
            if (logFileWriter != null) {
                try {
                    logFileWriter.write(logLineHeader);
                    logFileWriter.write(logContent);
                    logFileWriter.flush();
                } catch (IOException e) {
                    System.out.println("Failed to write to " + logType + " log: " + e.getMessage());
                    e.printStackTrace();
                    exit(1);
                }
            } else {
                System.out.println("appending to log failed");
                Thread.currentThread().getStackTrace();
                exit(1);
            }
        }

        public enum DataFusionLogType {
            ERROR, DML, SELECT
        }
    }
}
