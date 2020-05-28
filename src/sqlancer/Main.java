package sqlancer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;

import sqlancer.clickhouse.ClickhouseProvider;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.duckdb.DuckDBProvider;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mysql.MySQLProvider;
import sqlancer.postgres.PostgresProvider;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.tidb.TiDBProvider;

public final class Main {

    public static final File LOG_DIRECTORY = new File("logs");
    public static volatile AtomicLong nrQueries = new AtomicLong();
    public static volatile AtomicLong nrDatabases = new AtomicLong();
    public static volatile AtomicLong nrSuccessfulActions = new AtomicLong();
    public static volatile AtomicLong nrUnsuccessfulActions = new AtomicLong();

    static {
        if (!LOG_DIRECTORY.exists()) {
            LOG_DIRECTORY.mkdir();
        }
    }

    private Main() {
    }

    public static final class StateLogger {

        private final File loggerFile;
        private File curFile;
        private FileWriter logFileWriter;
        public FileWriter currentFileWriter;
        private static final List<String> INITIALIZED_PROVIDER_NAMES = new ArrayList<>();
        private boolean logEachSelect = true;
        private DatabaseProvider<?, ?> provider;

        private static final class AlsoWriteToConsoleFileWriter extends FileWriter {

            AlsoWriteToConsoleFileWriter(File file) throws IOException {
                super(file);
            }

            @Override
            public Writer append(CharSequence arg0) throws IOException {
                System.err.println(arg0);
                return super.append(arg0);
            }

            @Override
            public void write(String str) throws IOException {
                System.err.println(str);
                super.write(str);
            }
        }

        public StateLogger(String databaseName, DatabaseProvider<?, ?> provider, MainOptions options) {
            this.provider = provider;
            File dir = new File(LOG_DIRECTORY, provider.getDBMSName());
            if (dir.exists() && !dir.isDirectory()) {
                throw new AssertionError(dir);
            }
            ensureExistsAndIsEmpty(dir, provider);
            loggerFile = new File(dir, databaseName + ".log");
            logEachSelect = options.logEachSelect();
            if (logEachSelect) {
                curFile = new File(dir, databaseName + "-cur.log");
            }
        }

        private synchronized void ensureExistsAndIsEmpty(File dir, DatabaseProvider<?, ?> provider) {
            if (INITIALIZED_PROVIDER_NAMES.contains(provider.getDBMSName())) {
                return;
            }
            if (!dir.exists()) {
                try {
                    Files.createDirectories(dir.toPath());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            File[] listFiles = dir.listFiles();
            assert listFiles != null : "directory was just created, so it should exist";
            for (File file : listFiles) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
            INITIALIZED_PROVIDER_NAMES.add(provider.getDBMSName());
        }

        private FileWriter getLogFileWriter() {
            if (logFileWriter == null) {
                try {
                    logFileWriter = new AlsoWriteToConsoleFileWriter(loggerFile);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return logFileWriter;
        }

        public FileWriter getCurrentFileWriter() {
            if (!logEachSelect) {
                throw new UnsupportedOperationException();
            }
            if (currentFileWriter == null) {
                try {
                    currentFileWriter = new FileWriter(curFile, false);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return currentFileWriter;
        }

        public void writeCurrent(StateToReproduce state) {
            if (!logEachSelect) {
                throw new UnsupportedOperationException();
            }
            printState(getCurrentFileWriter(), state);
            try {
                currentFileWriter.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void writeCurrent(String queryString) {
            if (!logEachSelect) {
                throw new UnsupportedOperationException();
            }
            try {
                getCurrentFileWriter().write(queryString + ";\n");
                currentFileWriter.flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public void logRowNotFound(StateToReproduce state) {
            printState(getLogFileWriter(), state);
            try {
                getLogFileWriter().flush();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        public void logException(Throwable reduce, StateToReproduce state) {
            String stackTrace = getStackTrace(reduce);
            FileWriter logFileWriter2 = getLogFileWriter();
            try {
                logFileWriter2.write(stackTrace);
                printState(logFileWriter2, state);
            } catch (IOException e) {
                throw new AssertionError(e);
            } finally {
                try {
                    logFileWriter2.flush();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        private String getStackTrace(Throwable e1) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e1.printStackTrace(pw);
            String stackTrace = "--" + sw.toString().replace("\n", "\n--");
            return stackTrace;
        }

        private void printState(FileWriter writer, StateToReproduce state) {
            StringBuilder sb = new StringBuilder();
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            sb.append("-- Time: " + dateFormat.format(date) + "\n");
            sb.append("-- Database: " + state.getDatabaseName() + "\n");
            sb.append("-- Database version: " + state.getDatabaseVersion() + "\n");
            for (Query s : state.getStatements()) {
                if (s.getQueryString().endsWith(";")) {
                    sb.append(s.getQueryString());
                } else {
                    sb.append(s.getQueryString() + ";");
                }
                sb.append('\n');
            }
            if (state.getQueryString() != null) {
                sb.append(state.getQueryString() + ";\n");
            }
            try {
                writer.write(sb.toString());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            provider.printDatabaseSpecificState(writer, state);
        }

    }

    static int threadsShutdown;

    public static class QueryManager {

        private final Connection con;
        private final StateToReproduce stateToRepro;

        QueryManager(Connection con, StateToReproduce state) {
            if (con == null || state == null) {
                throw new IllegalArgumentException();
            }
            this.con = con;
            this.stateToRepro = state;

        }

        public boolean execute(Query q) throws SQLException {
            stateToRepro.statements.add(q);
            boolean success = q.execute(con);
            Main.nrSuccessfulActions.addAndGet(1);
            return success;
        }

        public void incrementSelectQueryCount() {
            Main.nrQueries.addAndGet(1);
        }

        public void incrementCreateDatabase() {
            Main.nrDatabases.addAndGet(1);
        }

    }

    public static void printArray(Object[] arr) {
        for (Object o : arr) {
            System.out.println(o);
        }
    }

    public static void main(String[] args) {
        List<DatabaseProvider<?, ?>> providers = new ArrayList<>();
        providers.add(new SQLite3Provider());
        providers.add(new CockroachDBProvider());
        providers.add(new MySQLProvider());
        providers.add(new MariaDBProvider());
        providers.add(new TiDBProvider());
        providers.add(new PostgresProvider());
        providers.add(new ClickhouseProvider());
        providers.add(new DuckDBProvider());
        Map<String, DatabaseProvider<?, ?>> nameToProvider = new HashMap<>();
        Map<String, Object> nameToOptions = new HashMap<>();
        MainOptions options = new MainOptions();
        Builder commandBuilder = JCommander.newBuilder().addObject(options);
        for (DatabaseProvider<?, ?> provider : providers) {
            String name = provider.getDBMSName();
            if (!name.toLowerCase().equals(name)) {
                throw new AssertionError(name + " should be in lowercase!");
            }
            Object command = provider.getCommand();
            if (command == null) {
                throw new IllegalStateException();
            }
            nameToProvider.put(name, provider);
            nameToOptions.put(name, command);
            commandBuilder = commandBuilder.addCommand(name, command);
        }
        JCommander jc = commandBuilder.programName("SQLancer").build();
        jc.parse(args);
        if (jc.getParsedCommand() == null) {
            jc.usage();
            System.exit(-1);
        }

        if (options.printProgressInformation()) {
            startProgressMonitor(options);
        }

        ExecutorService executor = Executors.newFixedThreadPool(options.getNumberConcurrentThreads());

        for (int i = 0; i < options.getTotalNumberTries(); i++) {
            final String databaseName = "database" + i;

            executor.execute(new Runnable() {

                StateToReproduce stateToRepro;
                StateLogger logger;
                DatabaseProvider<?, ?> provider;

                @Override
                public void run() {
                    runThread(databaseName);
                }

                private void runThread(final String databaseName) {
                    Thread.currentThread().setName(databaseName);
                    while (true) {
                        // create a new instance of the provider in case it has a global state
                        try {
                            provider = nameToProvider.get(jc.getParsedCommand()).getClass().newInstance();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        GlobalState<?> state = provider.generateGlobalState();
                        stateToRepro = provider.getStateToReproduce(databaseName);
                        state.setState(stateToRepro);
                        logger = new StateLogger(databaseName, provider, options);
                        Randomly r = new Randomly();
                        state.setRandomly(r);
                        state.setDatabaseName(databaseName);
                        state.setMainOptions(options);
                        Object dmbsSpecificOptions = nameToOptions.get(jc.getParsedCommand());
                        state.setDmbsSpecificOptions(dmbsSpecificOptions);
                        try (Connection con = provider.createDatabase(state)) {
                            QueryManager manager = new QueryManager(con, stateToRepro);
                            try {
                                java.sql.DatabaseMetaData meta = con.getMetaData();
                                stateToRepro.databaseVersion = meta.getDatabaseProductVersion();
                            } catch (SQLFeatureNotSupportedException e) {
                                // ignore
                            }
                            state.setConnection(con);
                            state.setStateLogger(logger);
                            state.setManager(manager);
                            Method method = provider.getClass().getMethod("generateAndTestDatabase", state.getClass());
                            method.setAccessible(true);
                            method.invoke(provider, state);
                        } catch (IgnoreMeException e) {
                            continue;
                        } catch (InvocationTargetException e) {
                            if (e.getCause() instanceof IgnoreMeException) {
                                continue;
                            } else {
                                e.getCause().printStackTrace();
                                stateToRepro.exception = e.getCause().getMessage();
                                logger.logFileWriter = null;
                                logger.logException(e.getCause(), stateToRepro);
                                threadsShutdown++;
                                break;
                            }
                        } catch (Throwable reduce) {
                            reduce.printStackTrace();
                            stateToRepro.exception = reduce.getMessage();
                            logger.logFileWriter = null;
                            logger.logException(reduce, stateToRepro);
                            threadsShutdown++;
                            break;
                        } finally {
                            try {
                                if (options.logEachSelect()) {
                                    if (logger.currentFileWriter != null) {
                                        logger.currentFileWriter.close();
                                    }
                                    logger.currentFileWriter = null;
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            if (threadsShutdown == options.getTotalNumberTries()) {
                                System.exit(-1);
                            }
                        }
                    }
                }
            });
        }

    }

    private static void startProgressMonitor(MainOptions options) {
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(new Runnable() {

            private long timeMillis = System.currentTimeMillis();
            private long lastNrQueries = 0;
            private long lastNrDbs;

            {
                timeMillis = System.currentTimeMillis();
            }

            @Override
            public void run() {
                long elapsedTimeMillis = System.currentTimeMillis() - timeMillis;
                long currentNrQueries = nrQueries.get();
                long nrCurrentQueries = currentNrQueries - lastNrQueries;
                double throughput = nrCurrentQueries / (elapsedTimeMillis / 1000d);
                long currentNrDbs = nrDatabases.get();
                long nrCurrentDbs = currentNrDbs - lastNrDbs;
                double throughputDbs = nrCurrentDbs / (elapsedTimeMillis / 1000d);
                long successfulStatementsRatio = (long) (100.0 * nrSuccessfulActions.get()
                        / (nrSuccessfulActions.get() + nrUnsuccessfulActions.get()));
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println(String.format(
                        "[%s] Executed %d queries (%d queries/s; %.2f/s dbs, successful statements: %2d%%). Threads shut down: %d.",
                        dateFormat.format(date), currentNrQueries, (int) throughput, throughputDbs,
                        successfulStatementsRatio, threadsShutdown));
                timeMillis = System.currentTimeMillis();
                lastNrQueries = currentNrQueries;
                lastNrDbs = currentNrDbs;
            }
        }, 5, 5, TimeUnit.SECONDS);
        if (options.getTimeoutSeconds() != -1) {
            scheduler.schedule(new Runnable() {

                @Override
                public void run() {
                    System.exit(0);
                }
            }, options.getTimeoutSeconds(), TimeUnit.SECONDS);
        }
    }

}
