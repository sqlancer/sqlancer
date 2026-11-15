package sqlancer.voltdb;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "VoltDB (default host: " + VoltDBOptions.DEFAULT_HOST
        + ", default port: " + VoltDBOptions.DEFAULT_PORT + ")")
public class VoltDBOptions implements DBMSSpecificOptions<VoltDBOracleFactory> {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 21212;

    @Parameter(names = "--oracle", description = "The test oracles to use")
    public List<VoltDBOracleFactory> oracles = List.of(VoltDBOracleFactory.NOREC);

    @Parameter(names = "--max-num-tables", description = "The maximum number of tables each database creates")
    public int maxNumTables = 3;

    @Parameter(names = "--max-num-indexes", description = "The maximum number of CREATE INDEX statements each database issues")
    public int maxNumIndexes = 4;

    @Parameter(names = "--max-num-updates", description = "The maximum number of UPDATE statements each database issues")
    public int maxNumUpdates = 5;

    @Parameter(names = "--max-num-deletes", description = "The maximum number of DELETE statements each database issues")
    public int maxNumDeletes = 2;

    @Parameter(names = "--max-num-views", description = "The maximum number of CREATE VIEW statements each database issues")
    public int maxNumViews = 2;

    @Parameter(names = "--retry-seconds", description = "How long a statement VoltDB rejects because of a concurrent schema change is retried")
    public int retrySeconds = 120;

    @Parameter(names = "--avoid-known-bugs", description = "Do not generate SQL that hits a known, unfixed VoltDB bug (see the README)", arity = 1)
    public boolean avoidKnownBugs = true;

    @Parameter(names = "--node-failures", description = "Nodes are killed and rejoined during the run (run-sqlancer.sh --kill-interval): expect and retry the errors that causes", arity = 1)
    public boolean nodeFailures;

    @Parameter(names = "--resizes", description = "The cluster grows and shrinks elastically during the run (run-sqlancer.sh --resize-interval): expect and retry the errors that hosts leaving the cluster cause, as for --node-failures", arity = 1)
    public boolean resizes;

    @Parameter(names = "--xdcr", description = "Comma-separated host:port client addresses of a second cluster linked to the first by XDCR (run-sqlancer.sh --xdcr): each database writes through one of the two, chosen at random, its DDL runs on both with DR TABLE for each table, and the XDCR oracle compares their rows")
    public String xdcr;

    @Parameter(names = "--upgrades", description = "The cluster is upgraded in service during the run, one node at a time (run-sqlancer.sh --upgrade-from): expect the errors that a stopped node causes, as for --node-failures", arity = 1)
    public boolean upgrades;

    @Parameter(names = "--export-target", description = "The export target (run-sqlancer.sh --export) that the STREAMS oracle's streams export to; without it, the streams export nowhere")
    public String exportTarget;

    @Parameter(names = "--security", description = "Security is on (run-sqlancer.sh --security): SQLancer connects as an administrator, the provider creates the roles sqlancer_read (SQLREAD) and sqlancer_write (SQL), and the PERMS oracle runs statements as the users reader and writer, who hold them", arity = 1)
    public boolean security;

    @Parameter(names = "--crashes", description = "Every node is killed at once and the cluster recovered from its command log during the run (run-sqlancer.sh --crash-interval): expect the errors that causes, as for --node-failures, and wait for the cluster to come back before connecting", arity = 1)
    public boolean crashes;

    @Parameter(names = "--ddl-hold", description = "A file that, while it exists, holds every DDL statement until it is gone (run-sqlancer.sh creates it during each shrink, ENG-30622); ignored with --avoid-known-bugs false")
    public String ddlHold;

    @Parameter(names = "--mirror", description = "Comma-separated host:port client addresses of a second cluster, normally running another VoltDB version: every statement that changes the database is repeated on it, and the DIFF oracle compares the two clusters' rows for the same query")
    public String mirror;

    @Parameter(names = "--servers", description = "Comma-separated host:port client addresses of every node; SQLancer connects to all of them, so it keeps working while any one node is down (used instead of --host and --port)")
    public String servers;

    @Override
    public List<VoltDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
